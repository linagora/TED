import express from "express";
import socketcluster from "socketcluster-client";
import TED, { HttpError } from "../index";

// First connect to TED and set options
TED.connect({
  options1: "",
  options2: "",
  credentials: {
    user: "",
    password: ""
  }
});

//We suppose we are in an existing application,
//so TED must be triggered with specific routes
const app = express();
app.all("/api/collections/*", async function(
  req: express.Request,
  res: express.Response
) {
  let collectionPath = req.method.replace("/api/collections/", "");

  //TED request should be as generic as possible
  // to be compatible with other Apis than express
  const response: any = await TED.request({
    path: collectionPath,
    body: req.body,
    originalRequest: req
  });

  //TODO add json response type
  res.send(response);
});

TED.beforeSave(
  "companies/channels/messages",
  (object: any, originalRequest: express.Request) => {
    object.date = new Date().getTime();

    if (object.content == "") {
      throw new HttpError("Content must not be empty.", 400); //Cancel operation and return HTTP 400 error
    }

    if (object.date < 0) {
      throw new Error("Date should not be like this."); //Cancel operation and return HTTP 500 error
    }

    return object;
  }
);

TED.afterSave(
  "companies/channels/messages",
  (objectBefore: any, objectAfter: any) => {
    if (objectBefore === null) {
      let channel = await TED.document("companies/channels", [
        "company-3",
        "channel-12"
      ]);
      channel.update({
        total_messages: total_messages + 1
      });
      await channel.save();
    }
  }
);
