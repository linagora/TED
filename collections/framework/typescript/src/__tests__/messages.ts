import express from "express";
import TED, { HttpError } from "../index";
import { delay } from "../TED/TedServer";
import {defaultTyping} from "../TED/Schemas";

let ted = new TED();

// First connect to TED and set options
ted.server.connect({
  url: "http://localhost:7250",
  username: "",
  password: "ceci est un mot de passe",
});

//We suppose we are in an existing application,
//so TED must be triggered with specific routes
const app = express();
app.use(express.json());
ted.bind(app, "/api/collections");

ted.before.save(
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

console.log("running");

ted.schemas.add("company",{
  fullsearchIndex:{
    defaultIndexation:false,
    defaultTyping: defaultTyping,
    keys:{
      "auteur":"wildcard",
      "content":"text"
    }
  },
  dbSearchIndex:{
    default:false,
    content:true,
    auteur:true,
    pouet:true,
    null:true,
    piege:false
  },
  wsPrivateKeys:{
    default:false
  }
});

ted.schemas.add("company/channel",{
  fullsearchIndex:{
    defaultIndexation:false,
    defaultTyping: defaultTyping,
    keys:{
      "auteur":"wildcard",
      "content":"text"
    }
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

ted.afterTasks(3);
