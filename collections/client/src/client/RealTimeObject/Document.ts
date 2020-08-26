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
    this.validatePrimaryKey(false);
  }

  /**
   * If the document was persisted, then we know the correct server id.
   * We can override our frontend generated identifier
   * @param id the new id
   */
  public updateTempPrimaryKey(id: string) {
    const idKey = this.collectionType.split("/").pop();
    if (idKey) {
      const tempIdentifier = this.getPrimaryKeyStringIdentifier({
        reduced: true,
      });
      this.collectionId[idKey] = id;

      const collection = this.tedInstance.collection(
        this.collectionType,
        this.collectionId
      );

      delete collection.documents[tempIdentifier];
      collection.documents[
        this.getPrimaryKeyStringIdentifier({
          reduced: true,
        })
      ] = this;

      return true;
    }
    return false;
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
}
