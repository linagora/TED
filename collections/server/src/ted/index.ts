import * as http from "http";
import handleRequest from "./core/macroRoutines/RequestHandling";
import * as myTypes from "./services/utils/myTypes";
import { mbInterface, fastForwardTaskStore, setup as mbSetup } from "./core/macroRoutines/StoredTaskHandling";
import { setup as cryptoSetup } from "./services/utils/cryptographicTools";
import { setup as cassandraSetup, client as cassandraClient} from "./services/database/adapters/cql/DatastaxTools";
import { TimerLogsMap, Timer, RequestTracker, RequestTrackerLog } from "./services/monitoring/Timer";
import { CounterMap } from "./services/monitoring/Counter";
import { setup as promSetup } from "./services/monitoring/PrometheusClient";
import * as config from "../config/config";
import * as promClient from "prom-client";

import { setup as setupSocketcluster } from "./services/socket/sockectServer";

export let globalTimerLogs:TimerLogsMap;
export let globalCounter:CounterMap;
export let globalTrackerLogs:RequestTrackerLog;

async function setup():Promise<void>
{
  if(config.sentry.sentry === true)
  {
    let Sentry = require("@sentry/node");
    Sentry.init({dsn: config.sentry.DSN});
  }
  globalTrackerLogs = new RequestTrackerLog();
  RequestTracker.logMap = globalTrackerLogs;
  globalTimerLogs = new TimerLogsMap();
  Timer.logMap = globalTimerLogs;
  globalCounter = new CounterMap();
  mbSetup();
  cryptoSetup();
  promSetup();
  await cassandraSetup();
}

async function getHTTPBody(req:http.IncomingMessage):Promise<myTypes.ServerRequestBody>
{
  return new Promise((resolve, reject) => 
  {
    try
    {
      let body:any[] = [];
      let body_str:string;
      req.on('data', (chunk:any) => 
      {
        body.push(chunk);
      }).on('end', () => 
      {
        body_str = Buffer.concat(body).toString();
        resolve(JSON.parse(body_str));
      });
    }
    catch
    {
      reject(new Error("failed parsing HTTP body"));
    }
  }) 
}

async function main():Promise<void>
{
  console.log("This is a highway to hell");
  let initTimer = new Timer("program_init");
  await setup();
  if(mbInterface !== undefined && mbInterface !== null) mbInterface.runTasks();
  else console.log("Running without task broker");
  await fastForwardTaskStore()
  .catch( (err:myTypes.CQLResponseError) =>
  {
    console.error(err);
    if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
    {
      console.log("TaskStore doesn't exist, nothing to fast forward.");
      return;
    }
    throw err;
  });

  console.log("Initializing http server");
  let httpServer = http.createServer(async function(req: http.IncomingMessage, res: http.OutgoingMessage)
  {
    try{
      if(req.url === "/metrics")
      {
        res.end(promClient.register.metrics());
        return;
      }
      console.log("\n\n ===== New Incoming Request =====");
      let httpTimer = new Timer("http_response");
      let operation:myTypes.ServerRequestBody = await getHTTPBody(req);
      let tracker = new RequestTracker("");
      try
      {
        let answer = await handleRequest(operation, "", undefined, tracker);
        res.write(JSON.stringify(answer));
        res.end();
      }
      catch(err)
      {
        console.log("catch2 \n",err);
        res.write('{"status":"' + err.toString() + '"}');
        res.end();
      }
      httpTimer.stop();
    }
    catch(err){
      console.error(err);
    }
  });
  setupSocketcluster(httpServer);
  httpServer.listen(8080);
  initTimer.stop();
}
main();
