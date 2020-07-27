import React, { useState } from "react";

function Message(props: any) {
  const [count, setCount] = useState(0);
  return (
    <div className="message" style={props.style}>
      <div>
        <div className="head" />
      </div>
      <div className="content">
        It's working 👍 ({props.index})<br />
        And also this{" "}
        <a
          href="#"
          onClick={() => {
            setCount(10);
            props.measureCache.clear(props.index);
            props.recomputeRowHeights(props.index);
          }}
        >
          Add
        </a>
        {Array.apply(null, Array(count))
          .map(function(_, i) {
            return i;
          })
          .map((_, i: number) => {
            return <div key={i}>{i}</div>;
          })}
      </div>
    </div>
  );
}

export default Message;
