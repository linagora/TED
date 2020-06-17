import React from "react";
import {
  AutoSizer,
  List,
  CellMeasurer,
  CellMeasurerCache
} from "react-virtualized";

let cache: any = new CellMeasurerCache({
  defaultHeight: 32,
  fixedWidth: true
});

function MessagesList() {
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
              rowCount={44}
              rowRenderer={({ index, key, style, parent }: any) => (
                <CellMeasurer
                  cache={cache}
                  columnIndex={0}
                  key={key}
                  parent={parent}
                  rowIndex={index}
                >
                  <div className="message" style={style}>
                    <div>
                      <div className="head" />
                    </div>
                    <div className="content">
                      It's working 👍 ({index})<br />
                      And also this
                    </div>
                  </div>
                </CellMeasurer>
              )}
            />
          )}
        </AutoSizer>
      </div>
    </div>
  );
}

export default MessagesList;
