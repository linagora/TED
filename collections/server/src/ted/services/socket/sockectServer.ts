import socketIO from "socket.io";
import https from "https";
import crypto from "crypto";
import { login, tedRequest, sendTasks } from "./procedures";
import { delay } from "../utils/divers";
import * as myTypes from "../utils/myTypes";

let io:socketIO.Server;

type AuthTable =
{
    [id:string]:boolean;
}
type SaltTable =
{
    [id:string]:Buffer;
}
export let authTable:AuthTable  = {};
export let saltTable:SaltTable = {};

export async function setup(httpsServer:https.Server):Promise<void>
{
    io = socketIO(httpsServer);
    io.on("connection", (socket) => 
    {
        console.log("init :", socket.id);

        let salt:Buffer = crypto.randomBytes(16);
        saltTable[socket.id] = salt;

        socket.emit("authenticate", salt, (hash:Buffer) => {login(socket, hash)})
        
        socket.on("tedRequest", async (data, callback) => 
        {
            if(! isAuth(socket))
            {
                let notAuthError = new Error("User not authentified");
                notAuthError.name = "notAuthError";
                console.error(notAuthError);
                callback(notAuthError, null);
                return;
            }
            await tedRequest(socket, data, callback);
        });

        socket.on("sendTasks", (prefetchCount:number) =>
        {
            sendTasks(socket, prefetchCount);
        });

        socket.on("disconnect", (reason) =>
        {
            delete saltTable[socket.id];
            delete authTable[socket.id];
            console.log(socket.id, " : disconnected");
        })
    });
}

function isAuth(client:socketIO.Socket):boolean
{
    return authTable[client.id] === undefined ? false : authTable[client.id];
}

export async function sendToSocket(event:string, data:any, afterSaveInfos:myTypes.AfterSaveInfos):Promise<void>
{
    return new Promise((resolve, reject) => 
    {
        try{
            console.log("sending afterSave");
            let socket = io.sockets.connected[afterSaveInfos.senderID];
            if(! isAuth(socket)) throw new Error("Socket not authenticated");
            socket.emit(event, data, () => resolve());
        }
        catch(err){
            reject(err);
        }
    });
}