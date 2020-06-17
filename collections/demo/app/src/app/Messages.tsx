import React from "react";
import MessagesList from "./MessagesList";

function Messages() {
  return (
    <div className="channel-view">
      <MessagesList />

      <div className="input">
        <textarea placeholder="Type a message when you're ready"></textarea>
      </div>
    </div>
  );
}

export default Messages;
