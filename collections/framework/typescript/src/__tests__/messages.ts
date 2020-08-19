import express from "express";
import TED, { HttpError } from "../index";

let ted = new TED();

// First connect to TED and set options
ted.server.connect({
  username: "",
  password: "ceci est un mot de passe"
});


//We suppose we are in an existing application,
//so TED must be triggered with specific routes
const app = express();
app.use(express.json());
ted.bind(app, "/api/collections/");


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
    default:false
  },
  dbSearchIndex:{
    default:false,
    content:true,
    auteur:true,
    null:true,
    piege:false
  },
  wsPrivateKeys:{
    default:false
  }
})