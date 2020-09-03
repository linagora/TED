import express from "express";
import TED, { HttpError } from "../index";
import { delay } from "../TED/TedServer";
import { DefaultTyping } from "../TED/Schemas";
import setSchemas from "./schemas";

export default (ted: TED) => {
  const app = express();
  app.use(express.json());
  ted.bind(app, "/api/collections");
  setSchemas(ted);

  ted.before.save(
    "company/channel/message",
    (requestedObject: any, originalRequest: express.Request) => {
      //Get current user
      console.log(originalRequest.headers);

      let object: any = {};
      object.creation_date = new Date().getTime();
      object.content = requestedObject.content || "";
      object.sender = null;

      return [object];
    }
  );

  ted.before.save(
    "channel/message",
    (object: any, originalRequest: express.Request) => {
      object.date = new Date().getTime();

      console.log("hello", object);

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

  console.log("running");

  ted.schemas.add("company", {
    fullsearchIndex: {
      defaultIndexation: false,
      defaultTyping: DefaultTyping,
      keys: {
        auteur: "wildcard",
        content: "text",
      },
    },
    dbSearchIndex: {
      default: false,
      content: true,
      auteur: true,
      pouet: true,
      null: true,
      piege: false,
    },
    wsPrivateKeys: {
      default: false,
    },
  });

  ted.schemas.add("company/channel", {
    fullsearchIndex: {
      defaultIndexation: false,
      defaultTyping: DefaultTyping,
      keys: {
        auteur: "wildcard",
        content: "text",
      },
    },
    dbSearchIndex: {
      default: false,
      content: true,
      auteur: true,
      pouet: true,
      null: true,
      piege: false,
    },
    wsPrivateKeys: {
      default: false,
    },
  });

  ted.after.save("company", async (object: Object) => {
    console.log("Aftersave : ", object);
    await delay(10000);
    console.log("end");
  });
  ted.after.remove("company", async (object: Object) => {
    console.log("Afterremove : ", object);
    await delay(10000);
    console.log("end");
  });
  ted.after.get("company", async (object: Object) => {
    console.log("Afterget : ", object);
    await delay(10000);
    console.log("end");
  });

  return app;
};
