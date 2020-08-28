import { FullsearchInterface } from "./FullsearchInterface";
import { ESinterface } from "./adapters/ElasticSearch/ESinterface";
import { ted } from "../../../config/config";
import { fullsearchRequest } from "../../core/macroRoutines/GetRoutine";

export let fullsearchInterface:FullsearchInterface;

export async function setup()
{
    switch(ted.fullsearch)
    {
        case "elasticsearch":
        {
            fullsearchInterface = new ESinterface();
            await fullsearchInterface.connect();
        }
    }
}