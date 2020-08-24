import ted from "../index";

ted.configure({
  env: "dev",
});

const messagesCollection = ted.collection("company/channel/message", {
  company: "",
  channel: "",
});

messagesCollection.subscribe((event) => {
  //Will send all the known data on the first call
  console.log(event);
});

messagesCollection.unsubscribe();

messagesCollection
  .search("some keyword" || { content: "some keywords" })
  .then((results) => {
    console.log(results);
  });

const messageDocument = ted.document("company/channel/message", {
  company: "",
  channel: "",
  message: "",
});

messageDocument.update({
  content: "coucou",
});

messageDocument.remove();

console.log("hello !");
