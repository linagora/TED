import socketIO from "socket.io-client";
import * as crypto from "crypto";
import { TedRequest } from "./DB";
import AfterOperation, { AfterTask } from "./AfterOperation";
import { resolve } from "path";
import { rejects } from "assert";

export type Credentials = {
  url: string;
  username: string;
  password: string;
};

type ExternalResolver = {
  res: () => void;
  rej: () => void;
};

let nullSocketError = new Error("TED socket has been deleted.");

export default class TEDServer {
  socket: SocketIOClient.Socket | null;
  salt: Buffer;
  loginPromise:Promise<void>;
  loginLock:ExternalResolver;
  after: AfterOperation;

  constructor(after: AfterOperation) {
    this.after = after;
    this.socket = null;
    this.salt = Buffer.alloc(16);

    let lock:ExternalResolver = {res:()=>{}, rej:()=>{}};
    this.loginPromise = new Promise((resolve, reject) =>
    {
      lock.res = resolve;
      lock.rej = reject;
    });
    this.loginLock = lock;
  }

  private initLoginLock()
  {
    let lock:ExternalResolver = {res:()=>{}, rej:()=>{}};
    this.loginPromise = new Promise((resolve, reject) =>
    {
      lock.res = resolve;
      lock.rej = reject;
    });
    this.loginLock = lock;
  }

  public async connect(credentials: Credentials): Promise<void> {
    this.socket = socketIO(credentials.url, {
      secure: true,
      rejectUnauthorized: false,
    });
    let that = this;

    this.socket.on("authenticate", async (salt: Buffer, login: any) => {
      let hash: Buffer = crypto.pbkdf2Sync(
        credentials.password,
        salt,
        1000,
        512,
        "sha512"
      );
      login(hash);
      await delay(100);
    });

    this.socket.on("disconnect", async (reason: string) => {
      that.initLoginLock();
      console.log("disconnected");
      await delay(1000);
      if (reason === "io server disconnect") {
        if (this.socket === null) throw nullSocketError;
        this.socket.connect();
      }
    });

    this.socket.on("reconnecting", (attemptNumber: number) => {
      console.log("reconnecting...");
    });

    this.socket.on("connect", () => {
      "socket connected, waiting for login...";
    });

    this.socket.on("loginSuccess", () => {
      console.log("Login successful");
      that.loginLock.res();
    });

    this.socket.on("loginFail", () => {
      console.log("Invalid credentials");
      that.loginLock.rej();
    });

    this.socket?.on("runTask", async (task: AfterTask, callback: any) => {
      try {
        await this.after.run(task);
        callback(null);
      } catch (err) {
        console.error(err);
        callback(err.message);
      }
    });
  }

  public async request(request: TedRequest): Promise<any> {
    await this.loginPromise;
    return new Promise((resolve, reject) => {
      try {
        console.log("sending :", request);
        if (this.socket === null) throw nullSocketError;
        if (!this.socket.connected)
          throw new Error(
            "TED currently disconnected, please try again after reconnection"
          );
        this.socket.emit("tedRequest", request, (err: any, result: any) => {
          if (err !== null) {
            let error = new Error("TED Error : " + err);
            console.error(error);
            reject(error);
          } else {
            console.log(result);
            resolve(result);
          }
        });
      } catch (err) {
        console.error(err);
        reject(err);
      }
    });
  }

  public async runTasks(prefetch: number): Promise<void> {
    await this.loginPromise;
    console.log("getting tasks...");

    this.socket?.emit("sendTasks", prefetch, async (err:any , data:any) =>
    {
      if(err !== null)
      {
        console.error(err);
      }
    });

    this.socket?.on("reconnect", () => {
      this.runTasks(prefetch);
    });
  }
}

export async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
