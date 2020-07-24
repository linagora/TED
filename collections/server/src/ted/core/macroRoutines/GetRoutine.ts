import * as myTypes from "../../services/utils/myTypes";
import { forwardCollection } from "./StoredTaskHandling";
import { globalCounter } from "../../index";
import { Timer, RequestTracker } from "../../services/monitoring/Timer";
import { GetMainView } from "../../services/database/operations/tedOperations/MainProjections";
import { getGetSecondaryView } from "../../services/database/operations/tedOperations/SecondaryProjections";

export let EmptyResultError = new Error("No matching object found");

export default async function getRequest(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<GetMainView>
{
    globalCounter.inc("get_precompute");
    let timer = new Timer("get_precompute");
    await forwardCollection(opDescriptor);
    tracker?.endStep("collection_update");

    if(opDescriptor.collections.length ===  opDescriptor.documents.length) return new GetMainView(opDescriptor);
    try
    {
        let options = opDescriptor.options as myTypes.GetOptions;
        if(options.where === undefined) return new GetMainView(opDescriptor);

        let matchingIDs:string[] = await getMatchingIDs(opDescriptor);
        tracker?.endStep("secondary_table_read");
        if(matchingIDs.length === 0) throw EmptyResultError;
        let op = buildGetOperation(opDescriptor, matchingIDs);
        timer.stop();
        return op;
    }
    catch(err)
    {
        if(err === EmptyResultError) throw err;
        return new GetMainView(opDescriptor);
    }
}

async function getMatchingIDs(opDescriptor:myTypes.InternalOperationDescription):Promise<string[]>
{
    let secondaryOp = getGetSecondaryView(opDescriptor);
    let objectKey:string = opDescriptor.collections.slice(-1)[0];
    let result:myTypes.ServerAnswer = await secondaryOp.execute();
    let matchingIDs:string[] = []

    if(result.queryResults === undefined || result.queryResults.allResultsClear === undefined) throw new Error("Unable to query requested fields : " + JSON.stringify(result));
    for(let object of result.queryResults.allResultsClear)
    {
        matchingIDs.push(object[objectKey] as string);
    }
    return matchingIDs;        
}

function buildGetOperation(opDescriptor:myTypes.InternalOperationDescription, matchingIDs: string[]):GetMainView
{
    let op = new GetMainView({
        action: myTypes.action.get,
        opID: opDescriptor.opID,
        documents: opDescriptor.documents,
        collections: opDescriptor.collections,
        options: {
            where: {
            field: opDescriptor.collections.slice(-1)[0],
            value: matchingIDs,
            operator: myTypes.Operator.in
            }
        }
    });
    return op
}