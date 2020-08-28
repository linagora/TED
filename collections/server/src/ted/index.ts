import * as http from "http";
import * as https from "https";
import handleRequest from "./core/macroRoutines/RequestHandling";
import * as myTypes from "./services/utils/myTypes";
import {
  mbInterface,
  fastForwardTaskStore,
  setup as mbSetup,
} from "./core/macroRoutines/StoredTaskHandling";
import { setup as cryptoSetup } from "./services/utils/cryptographicTools";
import { setup as cassandraSetup } from "./services/database/adapters/cql/DatastaxTools";
import { TimerLogsMap, Timer } from "./services/monitoring/Timer";
import { CounterMap } from "./services/monitoring/Counter";
import { setup as promSetup } from "./services/monitoring/PrometheusClient";
import { setup as mongoSetup } from "./services/database/adapters/sql/MongoDBtools";
import config from "./services/configuration/configuration";
import { setup as fullsearchSetup } from "./services/fullsearch/FullsearchSetup";
import * as promClient from "prom-client";
import * as socketIO from "socket.io";
import * as redisAdapter from "socket.io-redis";

import { setup as setupSocketcluster } from "./services/socket/sockectServer";

export let globalTimerLogs: TimerLogsMap;
export let globalCounter: CounterMap;

async function setup(): Promise<void> {
  if (config.configuration.sentry.sentry === true) {
    let Sentry = require("@sentry/node");
    Sentry.init({ dsn: config.configuration.sentry.DSN });
  }
  globalTimerLogs = new TimerLogsMap();
  Timer.logMap = globalTimerLogs;
  globalCounter = new CounterMap();
  mbSetup();
  cryptoSetup();
  promSetup();
  await fullsearchSetup();
  if (
    ["cassandra", "scylladb", "keyspace"].includes(
      config.configuration.ted.dbCore
    )
  )
    await cassandraSetup();
  else if (["mongodb"].includes(config.configuration.ted.dbCore))
    await mongoSetup();
}

async function getHTTPBody(
  req: http.IncomingMessage
): Promise<myTypes.ServerRequest> {
  return new Promise((resolve, reject) => {
    try {
      let body: any[] = [];
      let body_str: string;
      req
        .on("data", (chunk: any) => {
          body.push(chunk);
        })
        .on("end", () => {
          let body_str: any = {};
          try {
            let temp_body_str = Buffer.concat(body).toString();
            body_str = JSON.parse(temp_body_str);
          } catch (e) {
            body_str = {};
            console.error(e);
          }
          resolve(body_str);
        });
    } catch {
      reject(new Error("failed parsing HTTP body"));
    }
  });
}

export async function main(_args: any): Promise<void> {
  let initTimer = new Timer("program_init");
  await setup();
  if (mbInterface !== undefined && mbInterface !== null) mbInterface.runTasks();
  else console.log("Running without task broker");
  await fastForwardTaskStore().catch((err: myTypes.CQLResponseError) => {
    console.error(err);
    if (
      (err.code === 8704 &&
        err.message.substr(0, 18) === "unconfigured table") ||
      err.message.match(/^Collection ([a-zA-z_]*) does not exist./)
    ) {
      console.log("TaskStore doesn't exist, nothing to fast forward.");
      return;
    }
    throw err;
  });

  console.log("Initializing metrics server on 7251");
  let metricServer = http.createServer(async function (
    req: http.IncomingMessage,
    res: http.OutgoingMessage
  ) {
    if (req.url === "/metrics") {
      res.end(promClient.register.metrics());
      return;
    }
    res.end("go to /metrics");
  });
  metricServer.listen(7251);

  console.log("Initializing socket server on 7250");
  let httpServer = https.createServer({}, async function (
    req: http.IncomingMessage,
    res: http.OutgoingMessage
  ) {
    try {
      let httpTimer = new Timer("http_response");
      let operation: myTypes.ServerRequest = await getHTTPBody(req);
      try {
        let answer = await handleRequest(
          operation.body,
          operation.path,
          undefined
        );
        res.write(JSON.stringify(answer));
        res.end();
      } catch (err) {
        res.write('{"status":"' + err.toString() + '"}');
        res.end();
      }
      httpTimer.stop();
    } catch (err) {
      console.warn(err);
      res.end();
    }
  });
  setupSocketcluster(httpServer);
  httpServer.listen(7250);

  console.log("Initializing websocket server on 7252");
  let websocketsHttpServer = http.createServer({}, async function (
    _req: http.IncomingMessage,
    res: http.OutgoingMessage
  ) {
    res.end();
  });
  const websocketsServer: socketIO.Server = require("socket.io")(
    websocketsHttpServer
  );
  websocketsServer.on("connect", (socket) => {
    //TODO manage sockets
  });
  if (config.configuration.redis) {
    websocketsServer.adapter(
      require("socket.io-redis")({
        host: config.configuration.redis.host,
        port: config.configuration.redis.port,
      })
    );
  }
  websocketsHttpServer.listen(7252);

  initTimer.stop();
}
