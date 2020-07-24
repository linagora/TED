export default class TED {
  beforeSaves: any[] = [];

  /* Request from HTTP */
  connect() {}

  /* Request from HTTP */
  request(data: any) {}

  /* Create a beforeSave */
  beforeSave(
    collectionPath: string,
    callback: (object: any, originalRequest: any) => any
  ) {
    this.beforeSaves.push({
      collectionPath: collectionPath,
      callback: callback
    });
  }

  /* Create a afterSave */
  afterSave(
    collectionPath: string,
    callback: (object: any, originalRequest: any) => any
  ) {
    //TODO save callback somewhere
  }
}

export class HttpError extends Error {
  status = 500;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}
