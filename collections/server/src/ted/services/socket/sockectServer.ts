import socketIO from "socket.io";
import http from "http";
import { login, tedRequest } from "./procedures";
import { delay } from "../utils/divers";
import * as myTypes from "../utils/myTypes";

let io:socketIO.Server;

export async function setup(httpServer:http.Server):Promise<void>
{
    io = socketIO(httpServer);
    io.on("connection", (socket) => 
    {
        console.log("init :", socket.id);

        socket.on("login", (credentials, callback) => {login(socket, credentials, callback)});

        socket.on("tedRequest", async (data, callback)=> {
            if(! isAuth(socket))
            {
                let notAuthError = new Error("User not authentified");
                notAuthError.name = "notAuthError";
                socket.error(notAuthError);
            }
            await tedRequest(socket, data, callback);
        });
    });
}

function isAuth(client:socketIO.Socket):boolean
{
    return true;
}

export async function sendToSocket(event:string, data:any, afterSaveInfos:myTypes.AfterSaveInfos):Promise<void>
{
    return new Promise((resolve, reject) => 
    {
        try{
            console.log("sending afterSave");
            let socket = io.sockets.connected[afterSaveInfos.senderID];
            console.log(data);
            socket.emit(event, data, () => resolve());
        }
        catch(err){
            reject(err);
        }
    });
}