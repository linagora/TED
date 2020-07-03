import * as myTypes from "./myTypes";
import crypto, { Cipher, KeyObject } from "crypto";
import { SaveOperation, GetOperation } from "./BaseOperations";
import { clear } from "console";

/* export function encryptRequest(request:myTypes.ServerBaseRequest, key:crypto.KeyObject):void
{
  switch(request.action)
  {
    case myTypes.action.save:
    {
      if(request.object == undefined) throw new Error("missing field object in save request");
      request.object = encryptData({object: JSON.stringify(request.object)}, key);
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
} */

export function decryptResult(ans:myTypes.ServerAnswer, key:crypto.KeyObject):void
{
  if(ans.queryResults !== undefined && ans.queryResults.allResultsEnc !== undefined)
  {
    ans.queryResults.allResultsClear = [] as myTypes.ServerSideObject[];
    for(let i:number = 0; i<ans.queryResults.allResultsEnc.length; i++)
    {
      let encObject:myTypes.EncObject = JSON.parse(ans.queryResults.allResultsEnc[i].object);
      let clearData:myTypes.ServerSideObject = decryptData(encObject, key);
      let objectIDs:myTypes.ServerSideObject = ans.queryResults.allResultsEnc[i];
      objectIDs.object = clearData;

      ans.queryResults.allResultsClear.push(objectIDs);
    }
    ans.queryResults.allResultsEnc = undefined;
  }
}

export function encryptOperation(operation:myTypes.InternalOperationDescription, key:KeyObject):void
{
  if(operation.action === myTypes.action.save && operation.clearObject !== undefined) operation.encObject = JSON.stringify(encryptData(operation.clearObject, key));
  else if(operation.action === myTypes.action.batch && operation.operations !== undefined)
  {
    for(let op of operation.operations)
    {
      encryptOperation(op, key);
    }
  }
}

function encryptData(data:myTypes.ServerSideObject, key:KeyObject):myTypes.EncObject
{
  console.log(data);
  let iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  let encData:string = cipher.update(JSON.stringify(data), 'utf8', 'hex');
  encData += cipher.final('hex');
  let encObject:myTypes.EncObject = {data: encData, iv: iv.toString('base64'), auth: cipher.getAuthTag().toString('base64')};
  return encObject;
}

export function decryptData(encObject:myTypes.EncObject, key:KeyObject):myTypes.ServerSideObject
{
    if(encObject.iv === undefined) throw new Error("Unable to decrypt data, missing iv");
    if(encObject.auth === undefined) throw new Error("Unable to decrypt data, missing auth");
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(encObject.iv, 'base64'));
    decipher.setAuthTag(Buffer.from(encObject.auth, 'base64'));
    let clearData:string = decipher.update(encObject.data, 'hex', 'utf8');
    clearData += decipher.final('utf8');
    let clearObject:myTypes.ServerSideObject = JSON.parse(clearData);
    return clearObject;
}