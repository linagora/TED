import TED from "../index";
import setupApp from "./app";

let ted = new TED();

// First connect to TED and set options
ted.server.connect({
  url: "http://localhost:7250",
  username: "",
  password: "ceci est un mot de passe",
});

//We suppose we are in an existing application,
//so TED must be triggered with specific routes

let users = [];
let channels = [];
let companies = [];
let messages = [];

const app = setupApp(ted);

ted.afterTasks(3);
