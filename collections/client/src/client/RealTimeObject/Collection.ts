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
    this.validatePrimaryKey(true);
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
}
