import { FullsearchInterface } from "./FullsearchInterface";
import { ESinterface } from "./adapters/ElasticSearch/ESinterface";
import configuration from "../configuration/configuration";

export let fullsearchInterface: FullsearchInterface;

export async function setup() {
  switch (configuration.configuration.ted.fullsearch) {
    case "elasticsearch": {
      fullsearchInterface = new ESinterface();
      await fullsearchInterface.connect();
    }
  }
}
