import * as http from "http";
import handleRequest from "./MacroRoutines/RequestHandling";
import * as myTypes from "./BaseTools/myTypes";
import crypto from "crypto";
import { RedisLoop } from "./MacroRoutines/StoredTaskHandling";

//=================================================
//                TEST CODE

export const key:crypto.KeyObject = crypto.createSecretKey(crypto.createHash('sha256').update('test').digest());

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

http.createServer(async function(req: any, res: any)
{
  console.log("\n\n ===== New Incoming Request =====");11
  let body_str:myTypes.ServerBaseRequest = await getHTTPBody(req);
  try
  {
    console.log("Incoming request =\n", body_str);
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

console.log("This is a highway to hell");

RedisLoop();