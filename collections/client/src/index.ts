import RealTimeObject from "./RealTimeObject/RealTimeObject";
import Collection from "./RealTimeObject/Collection";
import Document from "./RealTimeObject/Document";

type DBOptions = {
  //TODO
}

export default class TwakeCollectionsClient {

  /**
   * Configure database connection
   */
  public configure(
    options: DBOptions
  ): void {
    //TODO
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
  ): RealTimeObject {
    return new Collection(type, primaryKey);
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
  ): RealTimeObject {
    return new Document(type, primaryKey);
  }

  /**
   * Be informed when network is lost and obtained again
   */
  public onStatusChange(
    callback: (status: boolean, reason: string) => void
  ): void {
    //TODO
  }
}
