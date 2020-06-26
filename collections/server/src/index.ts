import * as http from "http";
import createOperation from "./CQLOperations";
import * as myTypes from "./myTypes";
import crypto, { Cipher, KeyObject } from "crypto";
import { type } from "os";


function encryptRequest(request:myTypes.ServerBaseRequest, key:crypto.KeyObject):void
{
  switch(request.action)
  {
    case myTypes.action.save:
    {
      if(request.object == undefined) throw new Error("missing field object in save request");
      request.object = encryptData(request.object, key);
      break;
    }
    case myTypes.action.batch:
    {
      if(request.operations === undefined) throw new Error("missing field operations in batch request");
      for(let req of request.operations)
      {
        encryptRequest(req, key);
      }
      break;
    }
  }
}

function decryptResult(ans:myTypes.ServerAnswer, key:crypto.KeyObject):void
{
  if(ans.queryResults !== undefined)
  {
    for(let i:number = 0; i<ans.queryResults.resultCount; i++)
    {
      ans.queryResults.allResults[i] = decryptData(ans.queryResults.allResults[i], key);
    }
  }
}

function encryptData(data:myTypes.ClearObject, key:KeyObject):myTypes.EncObject
{
  let iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  let encData:string = cipher.update(data.content, 'utf8', 'hex');
  encData += cipher.final('hex');
  let encContent:myTypes.DBentry = {content: encData, iv: iv.toString('base64'), auth: cipher.getAuthTag().toString('base64')};
  let res:myTypes.EncObject = data;
  res.content = JSON.stringify(encContent);
  return res;
}

function decryptData(encObject:myTypes.EncObject, key:KeyObject):myTypes.ClearObject
{
  let encContent:myTypes.DBentry = JSON.parse(encObject.content);
  if(encContent.iv === undefined) throw new Error("Unable to decrypt data, missing iv");
  if(encContent.auth === undefined) throw new Error("Unable to decrypt data, missing auth");
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(encContent.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(encContent.auth, 'base64'));
  let clearData:string = decipher.update(encContent.content, 'hex', 'utf8');
  clearData += decipher.final('utf8');
  let clearObject:myTypes.ClearObject = encObject;
  clearObject.content = clearData;
  return clearObject;
}

//=================================================
//                TEST CODE

const key:crypto.KeyObject = crypto.createSecretKey(crypto.createHash('sha256').update('test').digest());

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
    console.log(body_str);
    encryptRequest(body_str, key);
    console.log(body_str);
    let op=createOperation(body_str);
    op.execute()
    .then( (result:myTypes.ServerAnswer) => 
    {
      decryptResult(result, key);
      res.write(JSON.stringify(result));
      res.end();
    })
    .catch((err:myTypes.ServerAnswer) => 
    {
      console.log("catch1", err);
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
