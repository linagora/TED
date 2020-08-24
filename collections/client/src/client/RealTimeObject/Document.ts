import RealTimeObject from "./RealTimeObject";
import { TedClient } from "..";

export default class Document extends RealTimeObject {
  constructor(
    type: string,
    primaryKey: {
      [key: string]: string;
    },
    tedClientInstance: TedClient
  ) {
    super(type, primaryKey, tedClientInstance);

    //Update primaryKey to remove any unwanted keys (all keys must correspond to type path)
    //Ex. For the document /company/channel/message (a specific message)
    //      we should define the keys {company: "", channel: "", message: ""}
    //    If message is not defined, an id will be generated in frontend (object added)
    //      in this case the id will take the form temp:some_id (old "front_id")
    //TODO and catch a warning if not enough keys (message is the only optionnal key)
  }

  /**
   * Update a document content
   * The object must contain a part or all of the saved object
   */
  public async update(_object: any) {
    //TODO
  }

  /**
   * Remove a document
   */
  public async remove() {
    //TODO
  }

  public getState() {
    //TODO
  }
}
