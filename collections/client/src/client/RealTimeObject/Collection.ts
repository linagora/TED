import RealTimeObject from "./RealTimeObject";
import Document from "./Document";
import { TedClient } from "..";

export default class Collection extends RealTimeObject {
  documents: { [key: string]: Document } = {};

  constructor(
    type: string,
    primaryKey: {
      [key: string]: string;
    },
    tedClientInstance: TedClient
  ) {
    super(type, primaryKey, tedClientInstance);

    //Update primaryKey to remove any unwanted keys (all keys must correspond to type path)
    //Ex. for the collection /company/channel/message (list of message in channel)
    //    we should define the keys {company: "", channel: ""} (message is not defined here because this is a collection of messages, not a message)
    //TODO and catch a warning if not enough keys
  }

  public async search(_query: any) {
    //TODO
    return [];
  }

  public async save(_document: Document) {
    //TODO
  }

  public async remove(_document: Document) {
    //TODO
  }

  public getState() {
    //TODO
  }
}
