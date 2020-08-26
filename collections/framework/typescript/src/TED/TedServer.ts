import socketIO from "socket.io-client";
import * as crypto from "crypto";
import { TedRequest } from "./DB";
import AfterOperation, { AfterTask } from "./AfterOperation";

const PORT = 8080;
const HOSTNAME = "localhost";
const TED_URL = "https://localhost:8080";

export type Credentials = {
  username: string;
  password: string;
};

let nullSocketError = new Error("TED socket has been deleted.");

export default class TEDServer {
  socket: SocketIOClient.Socket | null;
  salt: Buffer;
  logged: boolean;
  after: AfterOperation;

  constructor(after: AfterOperation) {
    this.after = after;
    this.socket = null;
    this.logged = false;
    this.salt = Buffer.alloc(16);
  }

  public async connect(credentials: Credentials): Promise<void> {
    this.socket = socketIO(TED_URL, {
      secure: true,
      rejectUnauthorized: false,
    });
    let that = this;

    this.socket.on("authenticate", (salt: Buffer, login: any) => {
      //let hmac = crypto.createHmac("sha512", salt);
      let hash: Buffer = crypto.pbkdf2Sync(
        credentials.password,
        salt,
        1000,
        512,
        "sha512"
      );
      login(hash);
    });

    this.socket.on("disconnect", async (reason: string) => {
      that.logged = false;
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
      that.logged = true;
    });

    this.socket.on("loginFail", () => {
      console.log("Invalid credentials");
      that.logged = false;
    });
  }

  public async request(request: TedRequest): Promise<any> {
    return new Promise((resolve, reject) => {
      try {
        console.log("sending :", request);
        if (this.socket === null) throw nullSocketError;
        if (!this.socket.connected)
          throw new Error(
            "TED currently disconnected, please try again after reconnection"
          );
        if (!this.logged) throw new Error("Not logged in");
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

  public runTasks(prefetch: number): void {
    console.log("getting tasks...");
    this.socket?.on("runTask", async (task: AfterTask, callback: any) => {
      try {
        await this.after.run(task);
        callback(null);
      } catch (err) {
        console.error(err);
        callback(err.message);
      }
    });

    this.socket?.emit("sendTasks", prefetch);

    this.socket?.on("reconnect", () => {
      this.socket?.emit("sendTasks", prefetch);
    });
  }
}

export async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
