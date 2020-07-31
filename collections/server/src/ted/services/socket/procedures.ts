import socketIO from "socket.io";
import handleRequest from "../../core/macroRoutines/RequestHandling";
import * as myTypes from "../utils/myTypes";

export async function login(socket:socketIO.Socket, credentials:any, callback:any):Promise<void>
{
    console.log("logged");
    callback("connection successful")
}

export async function tedRequest(socket:socketIO.Socket, data:any, callback:any):Promise<void>
{
    try{
        let result = await handleRequest(data.body, data.path, (data.afterSave === true ? {
            senderID: socket.id,
            originalRequest: data.originalRequest
        } : undefined));
        callback(result);
    }
    catch(err){
        socket.error(err);
    }
}