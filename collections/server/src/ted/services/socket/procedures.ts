import socketIO from "socket.io";
import handleRequest from "../../core/macroRoutines/RequestHandling";
import crypto from "crypto";
import { saltTable, authTable } from "./sockectServer";
import { ted } from "../../../config/config";
import * as myTypes from "../utils/myTypes";
import { delay } from "../utils/divers";

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
        let result = await handleRequest(data.body, data.path, (data.afterSave === true ? {
            senderID: socket.id,
            originalRequest: data.originalRequest
        } : undefined));
        callback(null, result);
    }
    catch(err){
        callback(err, null);
    }
}