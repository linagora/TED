import { TedClient } from ".";
import io from "socket.io-client";

export class Websockets {
  private tedClient: TedClient;

  public constructor(tedClient: TedClient) {
    this.tedClient = tedClient;
    this.connect();
  }

  public subscribe(_collection: string) {}

  public unsubscribe(_collection: string) {}

  public push(_collection: string, _data: JSON) {}

  private connect() {
    const socketUrl = this.tedClient.configuration.server.socket;

    console.log(socketUrl);

    const socket = io(socketUrl);

    socket.on("greetings", (res: any) => {
      console.log(res);
    });
  }
}
