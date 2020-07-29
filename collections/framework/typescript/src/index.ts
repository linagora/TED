import socketcluster from "socketcluster-client";
import { cpuUsage } from "process";

const PORT = 8000;
const HOSTNAME = "localhost";

type Credentials = {
  username:string,
  password:string
}

type Process = (object:any, originalRequest:any) => any;

type ProcessMap = {
  [path:string]:Process;
}

export default class TED {
  beforeSaves:ProcessMap = {};
  afterSaves:ProcessMap = {};
  socket:socketcluster.AGClientSocket;

  constructor()
  {
    this.socket = socketcluster.create({
      hostname: HOSTNAME,
      port: PORT
    });
  }

  /* Request from HTTP */
  public async connect(credentials:Credentials):Promise<void>
  {
    try{
      //await this.socket.invoke("login", credentials);
      console.log("Connection successful");
    }
    catch(err){
      console.log("Unable to connect");
      console.error(err);
    }

  }

  /* Request from HTTP */
  public async request(data: any):Promise<any>
  {
    let collectionPath = TED.getCollectionPath(data.path);
    if(this.beforeSaves[collectionPath] !== undefined)
    {
      data.body = this.beforeSaves[collectionPath](data.body, data.originalRequest);
    }
    try{
      console.log("sending");
      let result = await this.socket.invoke("aaa", data);
      console.log(result);
      return result;
    }
    catch(err){
      console.error(err);
    }

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