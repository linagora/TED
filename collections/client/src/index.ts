import Collection from "./RealTimeObject/Collection";
import Document from "./RealTimeObject/Document";

type DBOptions = {
  env: "dev" | "prod";
};

export class TedClient {
  configuration: DBOptions = {
    env: "prod",
  };

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
    return new Collection(type, primaryKey, this);
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
    return new Document(type, primaryKey, this);
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
