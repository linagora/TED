import React from "react";

function Channels() {
  return (
    <div className="channels-sidebar">
      <div className="title">Collections Demo</div>
      <div className="subtitle">CHAT</div>
      <div className="channel">
        <span className="icon">👋</span>Hello
      </div>
      <div className="channel selected">
        <span className="icon">🌈</span>Random
      </div>
    </div>
  );
}

export default Channels;
