import ted from "../client/index";
const sleep = (m: any) => new Promise((r) => setTimeout(r, m));

ted.configure({
  env: "dev",
  server: {
    http: "http://localhost:7250/",
    socket: "ws://localhost:7252/",
  },
});

(async () => {
  const messagesCollection = ted.collection("company/channel/message", {
    company: "3",
    channel: "14",
  });

  messagesCollection.subscribe((event) => {
    //Will send all the known data on the first call
    console.log(event);
  });

  await sleep(100);

  //Publish is used to send arbitrary data over an existing secured and authenticated realtime collection/document
  messagesCollection.publish("writing_user", {
    user: "1",
  });

  messagesCollection.unsubscribe();

  messagesCollection
    .search("some keyword" || { content: "some keywords" })
    .then((results) => {
      console.log(results);
    });

  const messageDocument = ted.document("company/channel/message", {
    company: "3",
    channel: "14",
    message: "209",
  });

  const newMessageDocument = ted.document("company/channel/message", {
    company: "3",
    channel: "14",
  });

  newMessageDocument.update({
    content: "Hello World",
  });

  messageDocument.remove();
})();
