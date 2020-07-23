import * as myTypes from "./myTypes";
import crypto from "crypto";
import { crypto as config } from "../Config/config";
import { globalCounter } from "./../index";
import { Timer } from "./../Monitoring/Timer";


export let globalKey:crypto.KeyObject;
export function setup()
{
  const password:Buffer = Buffer.from(config.password, "hex");
  const salt:Buffer = Buffer.from(config.salt, "hex");
  globalKey = crypto.createSecretKey(crypto.pbkdf2Sync(password, salt, 1000000, config.keyLen, "sha512")); 
}




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

export function encryptOperation(operation:myTypes.InternalOperationDescription, key:crypto.KeyObject):void
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

function encryptData(data:myTypes.ServerSideObject, key:crypto.KeyObject):myTypes.EncObject
{
  globalCounter.inc("encryption");
  let timer = new Timer("encryption");
  let iv = crypto.randomBytes(16);
  const cipher:crypto.Cipher = crypto.createCipheriv(config.algorithm, key, iv);
  let encData:string = cipher.update(JSON.stringify(data), 'utf8', 'hex');
  encData += cipher.final('hex');
  let encObject:myTypes.EncObject;
  if(isCipherGCM(cipher)) encObject = {data: encData, iv: iv.toString('base64'), auth: cipher.getAuthTag().toString('base64')};
  else encObject = {data: encData, iv : iv.toString("base64")};
  timer.stop();
  return encObject;
}

export function decryptData(encObject:myTypes.EncObject, key:crypto.KeyObject):myTypes.ServerSideObject
{
  globalCounter.inc("decryption");
  let timer = new Timer("decryption");
  if(encObject.iv === undefined) throw new Error("Unable to decrypt data, missing iv");
  if(encObject.auth === undefined) throw new Error("Unable to decrypt data, missing auth");
  const decipher:crypto.Decipher = crypto.createDecipheriv(config.algorithm, key, Buffer.from(encObject.iv, 'base64'));
  if(isDecipherGCM(decipher)) decipher.setAuthTag(Buffer.from(encObject.auth, 'base64'));
  let clearData:string = decipher.update(encObject.data, 'hex', 'utf8');
  clearData += decipher.final('utf8');
  let clearObject:myTypes.ServerSideObject = JSON.parse(clearData);
  timer.stop();
  return clearObject;
}

function isCipherGCM(cipher:crypto.Cipher): cipher is crypto.CipherGCM
{
  return (cipher as crypto.CipherGCM).getAuthTag !== undefined;
}

function isDecipherGCM(decipher:crypto.Decipher): decipher is crypto.DecipherGCM
{
  return (decipher as crypto.DecipherGCM).setAuthTag !== undefined;
}