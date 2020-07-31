import express from "express";
import TED, { HttpError } from "../index";

let ted = new TED();

// First connect to TED and set options
ted.connect({
  username: "",
  password: ""
});


//We suppose we are in an existing application,
//so TED must be triggered with specific routes
const app = express();
app.use(express.json());
app.put("/api/collections/*", async function(
  req: express.Request,
  res: express.Response
) 
{
  let collectionPath = req.url.replace("/api/collections/", "");
  //TED request should be as generic as possible
  // to be compatible with other Apis than express
  const response: any = await ted.save(
  {
    path: collectionPath,
    body: 
    {
      action: "save",
      object: req.body.object
    },
    originalRequest: req.rawHeaders
  });

  //TODO add json response type
  res.send(response);
});

app.get("/api/collections/*", async function(
  req: express.Request,
  res: express.Response
) 
{
  let path = req.url.replace("/api/collections/", "");
  console.log(req.body);
  //TED request should be as generic as possible
  // to be compatible with other Apis than express
  const response: any = await ted.get(
  {
    path: path,
    body: 
    {
      action: "get",
      where: req.body.where
    },
    originalRequest: req.rawHeaders
  });

  //TODO add json response type
  res.send(response);
});

app.delete("/api/collections/*", async function(
  req: express.Request,
  res: express.Response
) 
{
  let path = req.url.replace("/api/collections/", "");
  console.log(req.body);
  //TED request should be as generic as possible
  // to be compatible with other Apis than express
  const response: any = await ted.remove(
  {
    path: path,
    body: 
    {
      action: "remove",
    },
    originalRequest: req.rawHeaders
  });

  //TODO add json response type
  res.send(response);
});

ted.pushBeforeSave(
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

app.listen(9000);

/* ted.pushAfterSave(
  "companies/channels/messages",
  async (objectBefore: any, objectAfter: any) => {
    if (objectBefore === null) {
      let channel = await ted.document("companies/channels", [
        "company-3",
        "channel-12"
      ]);
      channel.update({
        total_messages: total_messages + 1
      });
      await channel.save();
    }
  }
); */