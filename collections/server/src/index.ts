import * as http from "http";
import * as utils from "./RequestHandling";
import { BaseOperation, SaveOperation } from "./BaseOperations";
import * as myTypes from "./myTypes";
import * as OperationLog from "./OperationsTable"
import crypto from "crypto";
import * as myCrypto from "./CryptographicTools";
import { type } from "os";

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
    let opDescriptor = utils.getInternalOperationDescription(body_str);
    console.log("opDescriptor =\n", opDescriptor);
    myCrypto.encryptOperation(opDescriptor, key);
    let op = await utils.createOperation(opDescriptor);
    if(op instanceof BaseOperation)
    {
      let opLog = new OperationLog.OperationLog(op);
      opLog.execute();
    }
    op.execute()
    .then( (result:myTypes.ServerAnswer) => 
    {
      myCrypto.decryptResult(result, key);
      res.write(JSON.stringify(result));
      res.end();
    })
    .catch((err:myTypes.ServerAnswer) => 
    {
      console.log("catch1 \n", err);
      res.write(JSON.stringify(err));
      res.end();
    });
  }
  catch(err)
  {
    console.log("catch2 \n",err);
    res.write('{"status":"' + err.toString() + '"}');
    res.end();
  }
  

}).listen(8080);

console.log("This is a highway to hell")
