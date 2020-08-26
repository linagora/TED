import * as myTypes from "../../services/utils/myTypes";
import { forwardCollection } from "./StoredTaskHandling";
import { globalCounter } from "../../index";
import { Timer } from "../../services/monitoring/Timer";
import { GetMainView } from "../tedOperations/MainProjections";
import { getGetSecondaryView } from "../tedOperations/SecondaryProjections";

export let EmptyResultError = new Error("No matching object found");

export default async function getRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<GetMainView>
{
    globalCounter.inc("get_precompute");
    let timer = new Timer("get_precompute");
    await forwardCollection(opDescriptor);
    if(opDescriptor.collections.length ===  opDescriptor.documents.length) return new GetMainView(opDescriptor);
    try
    {
        if(opDescriptor.secondaryInfos === undefined) return new GetMainView(opDescriptor);

        let matchingIDs:string[] = await getMatchingIDs(opDescriptor);
        timer.stop();
        if(matchingIDs.length === 0) throw EmptyResultError;
        let op = buildGetOperation(opDescriptor, matchingIDs);
        return op;
    }
    catch(err)
    {
        if(err === EmptyResultError) throw err;
        console.log(err);
        timer.stop();
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
            where:{
                key: opDescriptor.collections.slice(-1)[0],
                value: matchingIDs,
                operator: myTypes.Operator.in
            }
        }
    });
    return op
}