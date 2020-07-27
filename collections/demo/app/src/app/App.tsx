import React from "react";
import Channels from "./Channels";
import Messages from "./Messages";
import "./App.css";

import db from "twake-collections-client";

function App() {
  return (
    <div className="App">
      <Channels />
      <Messages />
    </div>
  );
}

export default App;
