import React, { useState } from "react";
import {
  AutoSizer,
  List,
  CellMeasurer,
  CellMeasurerCache
} from "react-virtualized";
import Message from "./Message";

let cache: any = new CellMeasurerCache({
  defaultHeight: 32,
  fixedWidth: true
});

let robotScroll: number = 0;
let attachToBottom = false;
let list: any = null;
let currentScrollTop = 0;

function MessagesList() {
  const [count, setCount] = useState(50);
  const onScroll = (
    clientHeight: number,
    scrollHeight: number,
    scrollTop: number
  ) => {
    currentScrollTop = scrollTop;
    if (new Date().getTime() - robotScroll > 100) {
      console.log("human scroll");
      if (scrollHeight === scrollTop + clientHeight) {
        attachToBottom = true;
        console.log("attach to bottom");
      } else {
        console.log("dettach from bottom");
        attachToBottom = false;
      }
      if (scrollTop === 0) {
        console.log("attach to top");
      }
    } else {
      console.log("robot scroll");
      if (attachToBottom) {
        console.log("force bottom");
        scrollToRow(Number.MAX_SAFE_INTEGER);
      }
    }
    robotScroll = 0;
  };
  const recomputeRowHeights = () => {
    robotScroll = new Date().getTime();
    list.recomputeRowHeights();
    if (attachToBottom) {
      scrollToRow(Number.MAX_SAFE_INTEGER);
    }
  };
  const scrollToRow = (count: number) => {
    robotScroll = 0;
    const scrollDestination = list.getOffsetForRow(count);

    //TODO animate this
    list.scrollToPosition(scrollDestination);
  };

  return (
    <div className="messages">
      <div className="list">
        <AutoSizer>
          {({ width, height }) => (
            <List
              rowHeight={cache.rowHeight}
              deferredMeasurementCache={cache}
              height={height}
              width={width}
              rowCount={count}
              ref={node => (list = node || list)}
              onScroll={({ clientHeight, scrollHeight, scrollTop }: any) =>
                onScroll(clientHeight, scrollHeight, scrollTop)
              }
              onRowsRendered={() => {
                console.log("rerendered");
                if (attachToBottom) {
                  scrollToRow(Number.MAX_SAFE_INTEGER);
                }
              }}
              rowRenderer={({ index, key, style, parent }: any) => (
                <CellMeasurer
                  cache={cache}
                  columnIndex={0}
                  key={key}
                  parent={parent}
                  rowIndex={index}
                >
                  <Message
                    style={style}
                    index={index}
                    measureCache={cache}
                    recomputeRowHeights={recomputeRowHeights}
                  />
                </CellMeasurer>
              )}
            />
          )}
        </AutoSizer>
      </div>
      <a
        onClick={() => {
          setCount(count + 1);
          if (attachToBottom) {
            scrollToRow(Number.MAX_SAFE_INTEGER);
          }
        }}
      >
        add
      </a>
    </div>
  );
}

export default MessagesList;
