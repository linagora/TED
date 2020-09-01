import TED, { StringIndexedObject } from "..";
import { SaveRequest, GetRequest, RemoveRequest } from "./DB";

export type BeforeProcess = (
  object: StringIndexedObject,
  request: any
) => StringIndexedObject;

type BeforeProcessMap = {
  [path: string]: BeforeProcess;
};

export default class BeforeOperation {
  saves: BeforeProcessMap = {};
  gets: BeforeProcessMap = {};
  removes: BeforeProcessMap = {};

  public async runSave(data: SaveRequest, originalRequest: any): Promise<any> {
    let collectionPath = TED.getCollectionPath(data.path);
    if (this.saves[collectionPath] !== undefined) {
      let final_data = data;
      let processed_object: any[] | any = this.saves[collectionPath](
        data.body.object,
        originalRequest
      );
      if (Array.isArray(processed_object)) {
        let _options = processed_object[1]; //TODO Not implemented
        let object = processed_object[0];
        final_data.body.object = object;
      }
      return final_data;
    }
    return data;
  }

  public async runGet(data: GetRequest, originalRequest: any): Promise<any> {
    let collectionPath = TED.getCollectionPath(data.path);
    if (this.gets[collectionPath] !== undefined)
      return this.gets[collectionPath](data, originalRequest);
    return data;
  }

  public async runRemove(
    data: RemoveRequest,
    originalRequest: any
  ): Promise<any> {
    let collectionPath = TED.getCollectionPath(data.path);
    if (this.removes[collectionPath] !== undefined)
      return this.saves[collectionPath](data, originalRequest);
    return data;
  }

  public save(path: string, callback: BeforeProcess) {
    this.saves[path] = callback;
  }

  public get(path: string, callback: BeforeProcess) {
    this.gets[path] = callback;
  }

  public remove(path: string, callback: BeforeProcess) {
    this.removes[path] = callback;
  }
}
