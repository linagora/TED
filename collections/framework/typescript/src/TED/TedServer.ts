import socketIO from "socket.io-client";
import { TedRequest, SaveRequest, GetRequest, RemoveRequest } from "./DB";
import AfterOperation from "./AfterOperation";

const PORT = 8080;
const HOSTNAME = "localhost";
const TED_URL = "http://localhost:8080";

export type Credentials = {
    username:string,
    password:string
}

let nullSocketError = new Error("TED socket has been deleted.");

export default class TEDServer
{
    socket:SocketIOClient.Socket | null;
    after:AfterOperation;

    constructor(after:AfterOperation)
    {
      this.after = after;
      this.socket = null;
    }

    public async connect(credentials:Credentials):Promise<void>
    {
        this.socket = socketIO(TED_URL);

        this.socket.on("afterSave", async (data:any, ack:any) =>
        {
            console.log(data);
            try{
                await this.after.runSave(data as SaveRequest);
                ack();
            }
            catch(err){
                console.error(err);
            }
        });

        this.socket.on("afterGet", async (data:any, ack:any) =>
        {
            console.log(data);
            try{
                await this.after.runGet(data as GetRequest);
                ack();
            }
            catch(err){
                console.error(err);
            }
        });

        this.socket.on("afterRemove", async (data:any, ack:any) =>
        {
            console.log(data);
            try{
                await this.after.runRemove(data as RemoveRequest);
                ack();
            }
            catch(err){
                console.error(err);
            }
        });
        
        this.socket.on("disconnect", (reason:string) => 
        {
            console.log("disconnected, trying to reconnect");
            if(reason === "io server disconnected") 
            {
                if(this.socket === null) throw nullSocketError;
                this.socket.connect();
            }
        });

        this.socket.on("connect", () =>
        {
            if(this.socket === null) throw nullSocketError;
            this.socket.emit("login", credentials, (result:any) =>
            {
                console.log(result);
            });
        });
    }

    public async request(request:TedRequest):Promise<any>
    {
      return new Promise((resolve, reject) =>
      {
        try{
          console.log("sending :", request);
          if(this.socket === null) throw nullSocketError;
          this.socket.emit("tedRequest", request, (result:any) => 
          {
            console.log(result);
            resolve(result);
          });
        }
        catch(err){
          console.error(err);
          reject(err);
        }
      });
    }
}