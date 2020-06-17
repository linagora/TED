import React from "react";

function MessagesList() {
  return (
    <div className="list">
      {[1, 2, 3, 4].map(item => (
        <div className="message">
          <div>
            <div className="head" />
          </div>
          <div className="content">It's working 👍 ({item})</div>
        </div>
      ))}
    </div>
  );
}

export default MessagesList;
