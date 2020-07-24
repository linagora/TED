import * as CQL from "../BaseTools/BaseOperations";
import * as myTypes from "./../BaseTools/myTypes";
import * as secondary from "../TEDOperations/SecondaryProjections";
import { forwardCollection } from "./StoredTaskHandling";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";

export let EmptyResultError = new Error("No matching object found");

export default async function getRequest(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<CQL.GetOperation>
{
    globalCounter.inc("get_precompute");
    let timer = new Timer("get_precompute");
    await forwardCollection(opDescriptor);
    tracker?.endStep("collection_update");

    if(opDescriptor.collections.length ===  opDescriptor.documents.length) return new CQL.GetOperation(opDescriptor);
    if(opDescriptor.secondaryInfos === undefined) return new CQL.GetOperation(opDescriptor);

    let matchingIDs:string[] = await getMatchingIDs(opDescriptor);
    tracker?.endStep("secondary_table_read");
    if(matchingIDs.length === 0) throw EmptyResultError;
    let op = buildGetOperation(opDescriptor, matchingIDs);
    timer.stop();
    return op;
}

async function getMatchingIDs(opDescriptor:myTypes.InternalOperationDescription):Promise<string[]>
{
    let secondaryOp = secondary.getGetOperationSecondaryIndex(opDescriptor);
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

function buildGetOperation(opDescriptor:myTypes.InternalOperationDescription, matchingIDs: string[]):CQL.GetOperation
{
    let op = new CQL.GetOperation({
        action: myTypes.action.get,
        opID: opDescriptor.opID,
        documents: opDescriptor.documents,
        collections: opDescriptor.collections,
        tableOptions: {secondaryTable: false},
        secondaryInfos: {
            field: opDescriptor.collections.slice(-1)[0],
            value: matchingIDs,
            operator: myTypes.Operator.in
        }
    });
    return op
}