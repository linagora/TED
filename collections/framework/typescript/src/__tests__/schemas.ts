import TED from "..";
import { DefaultTyping } from "../TED/Schemas";

export default (ted: TED) => {
  //Companies
  ted.schemas.add("company", {
    dbSearchIndex: {},
  });
  //Channels
  ted.schemas.add("company/channel", {
    dbSearchIndex: {},
  });
  //Messages
  ted.schemas.add("company/channel/message", {
    dbSearchIndex: {
      creation_date: true,
    },
  });
  //Users
  ted.schemas.add("user", {
    dbSearchIndex: {
      username: true,
      email: true,
    },
  });
};
