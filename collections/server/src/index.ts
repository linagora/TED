import * as http from "http";
import handleRequest from "./MacroRoutines/RequestHandling";
import * as myTypes from "./BaseTools/myTypes";
import { RedisLoop, fastForwardTaskStore } from "./MacroRoutines/StoredTaskHandling";
import { setup as cassandraSetup } from "./BaseTools/DatastaxTools";
import { setup as redisSetup } from "./BaseTools/RedisTools";


async function setup():Promise<void>
{
  await redisSetup();
  await cassandraSetup();
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
  });;
}

async function getHTTPBody(req:any):Promise<myTypes.ServerBaseRequest>
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
  await setup();
  http.createServer(async function(req: any, res: any)
  {
    console.log("\n\n ===== New Incoming Request =====");11
    let body_str:myTypes.ServerBaseRequest = await getHTTPBody(req);
    try
    {
      let answer = await handleRequest(body_str);
      res.write(JSON.stringify(answer));
      res.end();
    }
    catch(err)
    {
      console.log("catch2 \n",err);
      res.write('{"status":"' + err.toString() + '"}');
      res.end();
    }
  }).listen(8080);
  RedisLoop();
}
main();
