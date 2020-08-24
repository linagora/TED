import _ from "lodash";
import Document from "./Document";
import { TedClient } from "../index";

export default class RealTimeObject {
  private collectionType: string = "";
  private collectionId: { [key: string]: string } = {};
  private configuration: RealTimeObjectConfiguration = {};

  constructor(
    type: string,
    primaryKey: {
      [key: string]: string;
    },
    tedClientInstance: TedClient
  ) {
    this.collectionType = type;
    this.collectionId = JSON.parse(JSON.stringify(primaryKey));

    //Debug collections in the console
    if (window && tedClientInstance.configuration.env === "dev") {
      (window as any).ted_collections = (window as any).ted_collections || {};
      const ted_collections = (window as any).ted_collections;
      ted_collections[this.collectionType] =
        ted_collections[this.collectionType] || {};
      ted_collections[this.collectionType][
        Object.keys(this.collectionId)
          .map((key) => key + "=" + this.collectionId[key])
          .join(";")
      ] = this;
    }
  }

  public getPrimaryKeyStringIdentifier(primaryKey: { [key: string]: string }) {
    return Object.keys(primaryKey)
      .map((item) => item + "-" + primaryKey[item])
      .join("-");
  }

  /**
   * Set Object real time configuration
   * @param options : see RealTimeObjectConfiguration
   */
  public configure(options: RealTimeObjectConfiguration) {
    Object.assign(this.configuration, options);
  }

  protected getConfiguration(key: string): any {
    return _.get(this.configuration, key);
  }

  public subscribe(_callback: (objects: Document[]) => any): any {
    //TODO
  }

  public unsubscribe() {
    //TODO
  }

  public publish(_type: string, _event: any) {
    //TODO: publish arbitrary data on this channel (ex. is writting data)
  }
}

type RealTimeObjectConfiguration = {
  offline?: {
    write?: boolean; //Allow offline write
    read?: boolean; //Allow offline read
  };
  undo?: {
    allow?: boolean; //Allow undo / redo function
  };
};
