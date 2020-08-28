import config from "./ted/services/configuration/configuration";

const args: any = {};
let previousArgument: string | null = "";
try {
  (
    JSON.parse(((process.env || {}).npm_config_argv as any) || {}).original ||
    []
  ).forEach((element: string) => {
    if (element.indexOf("--") === 0) {
      previousArgument = element.split("--")[1];
    } else if (element.indexOf("-") === 0) {
      previousArgument = null;
      args[element] = true;
    } else {
      if (previousArgument) {
        args[previousArgument] = element;
      }
    }
  });
} catch (e) {
  console.log(e);
}

config.setup(args.config);

import { main } from "./ted/index";
main(args);
