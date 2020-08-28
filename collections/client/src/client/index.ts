import Collection from "./RealTimeObject/Collection";
import Document from "./RealTimeObject/Document";

type DBOptions = {
  env: "dev" | "prod";
  server: {
    http: string;
    socket: string;
  };
};

export class TedClient {
  configuration: DBOptions = {
    env: "prod",
    server: {
      http: "http://localhost:7250/",
      socket: "ws://localhost:7251/",
    },
  };

  collections: { [key: string]: Collection } = {};

  /**
   * Configure database connection
   */
  public configure(options: { [K in keyof DBOptions]?: DBOptions[K] }): void {
    this.configuration = Object.assign(this.configuration, options);
  }

  /**
   * Retrieve a Collection object
   * type: "message" or "channel" for example
   * filter: {
   *   "channelId": "some-uuidv4-id"
   * }
   */
  public collection(
    type: string,
    primaryKey: {
      [key: string]: string;
    }
  ): Collection {
    const collection = new Collection(type, primaryKey, this);
    const collectionKeyIdentifier = collection.getPrimaryKeyStringIdentifier({
      reduced: true,
    });

    //Return the pre-existing collection if exists
    this.collections[type] = this.collections[type] || {};
    if (this.collections[type][collectionKeyIdentifier]) {
      return this.collections[type][collectionKeyIdentifier];
    }
    console.log("create collection ", collectionKeyIdentifier);
    this.collections[type][collectionKeyIdentifier] = collection;

    return collection;
  }

  /**
   * Retrieve a Document object
   * type: "message" or "channel" for example
   * filter: {
   *   "channelId": "some-uuidv4-id",
   *   "id": "some-uuidv4-id"
   * }
   */
  public document(
    type: string,
    primaryKey: {
      [key: string]: string;
    }
  ): Document {
    const parent_collection = this.collection(type, primaryKey);

    const document = new Document(type, primaryKey, this);
    const documentKeyIdentifier = document.getPrimaryKeyStringIdentifier({
      reduced: true,
    });

    if (parent_collection.documents[documentKeyIdentifier]) {
      return parent_collection.documents[documentKeyIdentifier];
    }
    parent_collection.documents[documentKeyIdentifier] = document;

    return document;
  }

  /**
   * Be informed when network is lost and obtained again
   */
  public onStatusChange(
    _callback: (status: boolean, reason: string) => void
  ): void {
    //TODO
  }
}

export default new TedClient();
