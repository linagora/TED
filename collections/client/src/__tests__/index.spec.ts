import ted from "../client/index";

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

//Publish is used to send arbitrary data over an existing secured and authenticated realtime collection/document
messagesCollection.publish("writing_user", {
  user: "some_id",
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
