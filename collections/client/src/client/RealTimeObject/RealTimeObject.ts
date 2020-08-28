import _ from "lodash";
import Document from "./Document";
import { TedClient } from "..";
import { v4 as uuidv4 } from "uuid";

export default class RealTimeObject {
  protected collectionType: string = "";
  protected collectionId: { [key: string]: string } = {};
  protected configuration: RealTimeObjectConfiguration = {};
  protected tedInstance: TedClient;
  protected state: RealTimeObjectState = {
    sent: false,
    received: false,
    synced: false,
    currentVersion: "",
  };

  constructor(
    type: string,
    primaryKey: {
      [key: string]: string;
    },
    tedClientInstance: TedClient
  ) {
    this.collectionType = type;
    this.collectionId = JSON.parse(JSON.stringify(primaryKey));
    this.tedInstance = tedClientInstance;

    //Debug collections in the console
    if (window && tedClientInstance.configuration.env === "dev") {
      (window as any).ted_collections = (window as any).ted_collections || {};
      const ted_collections = (window as any).ted_collections;
      ted_collections[this.collectionType] =
        ted_collections[this.collectionType] || {};
      ted_collections[this.collectionType][
        this.getPrimaryKeyStringIdentifier()
      ] = this;
    }
  }

  //Update primaryKey to remove any unwanted keys (all keys must correspond to type path)
  //Ex. For the document /company/channel/message (a specific message)
  //      we should define the keys {company: "", channel: "", message: ""}
  //    If 'message' is not defined, an id will be generated in frontend (object added)
  //      in this case the id will take the form temp:some_id (old "front_id")
  public validatePrimaryKey(isCollection: boolean = false) {
    const needed = ("/" + this.collectionType + "/")
      .split("/")
      .filter((a) => a); //Need all elements
    let lastKey: string | false = needed[needed.length - 1];

    if (isCollection) {
      needed.pop(); //Collections doesnt need the last key to be defined
      lastKey = false;
    }
    const finalPrimaryKey = {};
    Object.keys(this.collectionId).forEach((key) => {
      if (needed.indexOf(key) >= 0) {
        finalPrimaryKey[key] = this.collectionId[key];
      }
    });
    if (lastKey && !finalPrimaryKey[lastKey]) {
      finalPrimaryKey[lastKey] = "temp:" + uuidv4();
    }
    if (needed.length != Object.keys(finalPrimaryKey).length) {
      console.error(
        "The primary key for collection is not valid (need " +
          needed.length +
          " keys, found " +
          Object.keys(finalPrimaryKey).length +
          ").",
        this
      );
      return false;
    }
    this.collectionId = JSON.parse(JSON.stringify(finalPrimaryKey));
    return true;
  }

  public getPrimaryKeyStringIdentifier(options: { reduced?: boolean } = {}) {
    const primaryKey = this.collectionId;
    const type = this.collectionType;
    return type
      .split("/")
      .filter((a) => a)
      .map((item) => {
        if (!(primaryKey || {})[item]) {
          return options.reduced ? "" : item;
        } else {
          return (options.reduced ? "" : item + "/") + (primaryKey || {})[item];
        }
      })
      .filter((a) => a)
      .join("/");
  }

  public getPrimaryKey() {
    return this.collectionId;
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

  public getState() {
    return this.state;
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
};

type RealTimeObjectState = {
  currentVersion: string; //Last operationID corresponding to this document
  sent: boolean; //Documents only: When the server respond with the final object
  received: boolean; //Documents only: When we receive the object from the collection websockets
  synced: boolean; //The element was synced from the server at least once
};
