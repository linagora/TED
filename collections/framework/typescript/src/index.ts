import socketIO from "socket.io-client";

const PORT = 8080;
const HOSTNAME = "localhost";
const TED_URL = "http://localhost:8080";

type Credentials = {
  username:string,
  password:string
}

type Process = (object:Object, request:any) => Object;

type ProcessMap = {
  [path:string]:Process;
}

export default class TED {
  beforeSaves:ProcessMap = {};
  afterSaves:ProcessMap = {};
  socket:SocketIOClient.Socket;

  constructor()
  {
    this.socket = socketIO(TED_URL);
  }

  /* Request from HTTP */
  public async connect(credentials:Credentials):Promise<void>
  {
    this.socket.on("disconnect", (reason:string) => 
    {
      console.log("disconnected, trying to reconnect");
      if(reason === "io server disconnected") this.socket.connect();
    });

    this.socket.on("connect", () =>
    {
      this.socket.emit("login", credentials, (result:any) =>
      {
        console.log(result);
      });
    });

    this.socket.on("afterSave", (data:any, ack:any) =>
    {
      console.log(data);
      ack();
      let collectionPath = TED.getCollectionPath(data.path);
      if(this.afterSaves[collectionPath] !== undefined)
      {
        this.afterSaves[collectionPath](data.object, data.originalRequest);
      } 
    })
  }

  /* Request from HTTP */
  public async save(data: any):Promise<any>
  {
    let collectionPath = TED.getCollectionPath(data.path);
    if(this.beforeSaves[collectionPath] !== undefined)
    {
      data.body = this.beforeSaves[collectionPath](data.body, data.originalRequest);
    }
    if(this.afterSaves[collectionPath] !== undefined || true)
    {
      data.afterSave = true;
    }
    return this.request(data)
  }

  public async get(data:any):Promise<any>
  {
    return this.request(data);
  }

  public async remove(data:any):Promise<any>
  {
    return this.request(data);
  }

  private async request(request:any):Promise<any>
  {
    return new Promise((resolve, reject) =>
    {
      try{
        console.log("sending :", request);
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

  /* Create a beforeSave */
  public pushBeforeSave(path:string, callback:Process):void
  {
    let collectionPath = TED.getCollectionPath(path);
    this.beforeSaves[collectionPath] = callback;
  }

  /* Create an afterSave */
  public pushAfterSave(path:string, callback:Process)
  {
    let collectionPath = TED.getCollectionPath(path);
    this.afterSaves[collectionPath] = callback;
  }

  protected static getCollectionPath(path:string):string
  {
    let elems = path.split("/");
    let res:string[] = [];
    for(let i:number = 0; i < elems.length; i+=2) res.push(elems[i]);
    return res.join("/");
  }
}

export class HttpError extends Error {
  status = 500;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}