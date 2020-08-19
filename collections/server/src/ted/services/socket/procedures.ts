import socketIO from "socket.io";
import handleRequest from "../../core/macroRoutines/RequestHandling";
import crypto from "crypto";
import { saltTable, authTable } from "./sockectServer";
import { ted } from "../../../config/config";
import * as myTypes from "../utils/myTypes";
import { delay } from "../utils/divers";
import * as config from "./../../../config/config";
import { RabbitMQBroker, SQSBroker, TaskBroker } from "./../messageBroker/MessageBrokerInterface";

export async function login(socket:socketIO.Socket, hash:Buffer):Promise<void>
{
    let salt:Buffer = saltTable[socket.id];
    let target = crypto.pbkdf2Sync(ted.password, salt, 1000, 512, "sha512");
    if(target.compare(hash) !== 0)
    {
        //callback(new Error("Invalid password"), null);
        authTable[socket.id] = false;
        console.log(socket.id, " : authentication failed");
        socket.emit("loginFail");
        await delay(100);
        socket.disconnect(true);
    }
    else
    {
        authTable[socket.id] = true;
        //callback(null, "authentication successful")
        console.log(socket.id, " : authentication successful");
        socket.emit("loginSuccess");
    }
}

export async function tedRequest(socket:socketIO.Socket, data:any, callback:any):Promise<void>
{
    try{
        let result = await handleRequest(data.body, data.path, data.afterTask);
        callback(null, result);
    }
    catch(err){
        console.error(err);
        callback(err.message, null);
    }
}

export async function sendTasks(socket:socketIO.Socket, prefetchCount:number):Promise<void>
{
    let computeTask = async function (task:string):Promise<void>
    {
        return new Promise((resolve, reject) =>
        {
            try
            {
                socket.emit("runTask", JSON.parse(task), (err:any) =>
                {
                    if(err !== null)
                    {
                        let error = new Error("Framework Error : " + err);
                        reject(error);
                    }
                    else{
                        resolve();
                    }
                });

                socket.on("disconnect", (reason:any) => {reject(new Error("Socket disconnected : " + reason))})
            }
            catch(error)
            {
                reject(error);
            }
        });
    };
    let broker = setupBroker(computeTask, prefetchCount);
    broker.runTasks();
}

function setupBroker(callback:any, prefetchCount:number):TaskBroker
{
    switch(config.ted.broker)
    {
        case "RabbitMQ":
        {
            return new RabbitMQBroker(config.rabbitmq.afterTaskBroker.URL, config.rabbitmq.afterTaskBroker.queueName, callback, config.rabbitmq.afterTaskBroker.queueOptions, config.rabbitmq.afterTaskBroker.messageOptions, config.rabbitmq.afterTaskBroker.rejectionTimeout, prefetchCount);
        }
        case "SQS":
        {
            return new SQSBroker(callback, config.sqs.afetrTaskBroker.queueOptions, config.sqs.afetrTaskBroker.messageOptions, prefetchCount);
        }
        default:
        {
            throw new Error("Unknow broker in config file");
        }
    }
}