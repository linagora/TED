import * as socketIO from "socket.io";
import handleRequest from "../../core/macroRoutines/RequestHandling";
import * as crypto from "crypto";
import { saltTable, authTable } from "./sockectServer";
import config from "../../services/configuration/configuration";
import * as myTypes from "../utils/myTypes";
import { delay } from "../utils/divers";
import {
  RabbitMQBroker,
  SQSBroker,
  TaskBroker,
} from "./../messageBroker/MessageBrokerInterface";
import { Timer } from "../monitoring/Timer";

export async function login(
  socket: socketIO.Socket,
  hash: Buffer
): Promise<void> {
  let salt: Buffer = saltTable[socket.id];
  let target = crypto.pbkdf2Sync(
    config.configuration.ted.password,
    salt,
    1000,
    512,
    "sha512"
  );
  if (target.compare(hash) !== 0) {
    authTable[socket.id] = false;
    console.log(socket.id, " : authentication failed");
    socket.emit("loginFail");
    await delay(50);
    socket.disconnect(true);
  } 
  else {
    authTable[socket.id] = true;
    console.log(socket.id, " : authentication successful");
    socket.emit("loginSuccess");
  }
}

export async function tedRequest(
  socket: socketIO.Socket,
  data: any,
  callback: any
): Promise<void> {
  let timer = new Timer("socket_response");
  try {
    let result = await handleRequest(data.body, data.path, data.afterTask);
    callback(null, result);
    timer.stop();
  } catch (err) {
    console.error(err);
    callback(err.message, null);
    timer.stop();
  }
}

export async function sendTasks(
  socket: socketIO.Socket,
  prefetchCount: number
): Promise<void> {
  let computeTask = async function (task: string): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        socket.emit("runTask", JSON.parse(task), (err: any) => {
          if (err !== null) {
            let error = new Error("Framework Error : " + err);
            reject(error);
          } else {
            resolve();
          }
        });
      } catch (error) {
        reject(error);
      }
    });
  };
  let broker = setupBroker(computeTask, prefetchCount);
  broker.runTasks();
  socket.on("disconnect", (reason: any) => {
    broker.stops();
    console.log(socket.id, " : socket disconnected");
  });
}

function setupBroker(callback: any, prefetchCount: number): TaskBroker {
  switch (config.configuration.ted.broker) {
    case "RabbitMQ": {
      return new RabbitMQBroker(
        config.configuration.rabbitmq.url,
        config.configuration.rabbitmq.afterTaskBroker.queueName,
        callback,
        config.configuration.rabbitmq.afterTaskBroker.queueOptions,
        config.configuration.rabbitmq.afterTaskBroker.messageOptions,
        config.configuration.rabbitmq.afterTaskBroker.rejectionTimeout,
        prefetchCount
      );
    }
    case "SQS": {
      return new SQSBroker(
        callback,
        config.configuration.sqs.afetrTaskBroker.queueOptions,
        config.configuration.sqs.afetrTaskBroker.messageOptions,
        prefetchCount,
        false
      );
    }
    default: {
      throw new Error("Unknow broker in config file");
    }
  }
}
