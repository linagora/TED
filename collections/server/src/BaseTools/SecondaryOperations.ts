import * as myTypes from "./myTypes";
import * as CQL from "./BaseOperations";
import { createHash } from "crypto";


export let TStoCQLtypes:Map<string, string>= new Map();
TStoCQLtypes.set("string", "text");
TStoCQLtypes.set("number", "decimal");
TStoCQLtypes.set("boolean", "Boolean");

export function getSaveOperationSecondaryIndex(operation:myTypes.InternalOperationDescription, secondaryKey:string):CQL.SaveOperation
{
    if(operation.clearObject === undefined) throw new Error("Missing object for a secondary index update")
    let op = new CQL.SaveOperation({
        action: myTypes.action.save,
        opID: operation.opID,
        collections: operation.collections,
        documents: operation.documents,
        encObject: operation.documents.slice(-1)[0],
        secondaryInfos: createSecondaryInfos(operation.clearObject, secondaryKey),
        tableOptions:{secondaryTable:true}
    });
    return op;
}

export function getRemoveOperationSecondaryIndex(operation:myTypes.InternalOperationDescription, where?:myTypes.WhereClause):CQL.RemoveOperation
{
    if(operation.secondaryInfos === undefined && where === undefined) throw new Error("Unable to find an object in a secondary table without a condition");
    let op= new CQL.RemoveOperation({
        action: myTypes.action.remove,
        opID: operation.opID,
        collections : operation.collections,
        documents: operation.documents,
        secondaryInfos: where === undefined ? operation.secondaryInfos : where,
        tableOptions: {secondaryTable: true}
    });
    return op;
}

export function getGetOperationSecondaryIndex(operation:myTypes.InternalOperationDescription, where?:myTypes.WhereClause):CQL.GetOperation
{
    if(operation.secondaryInfos === undefined && where === undefined) throw new Error("Unable to find an object in a secondary table without a condition");
    let op = new CQL.GetOperation({
        action: myTypes.action.get,
        opID: operation.opID,
        collections: operation.collections,
        documents: operation.documents,
        secondaryInfos: where === undefined ? operation.secondaryInfos : where,
        tableOptions: {secondaryTable:true}
    });
    return op;
}

export function createSecondaryInfos(object:myTypes.ServerSideObject, secondaryKey:string, operator:myTypes.Operator = myTypes.Operator.eq):myTypes.WhereClause
{
    if(object[secondaryKey] === undefined) throw new Error("Secondary key doesn't exist in object");
    if(typeof(object[secondaryKey]) === "string")
    {
        return {
            field:secondaryKey,
            value: createHash("sha256").update(object[secondaryKey] as string).digest('base64'),
            operator: operator
        }
    }
    return {
        field: secondaryKey,
        value: object[secondaryKey],
        operator: operator
    }
}

export function removePreviousValue(opDescriptor:myTypes.InternalOperationDescription, where:myTypes.WhereClause):CQL.RemoveOperation
{
    if( opDescriptor.collections.length !== opDescriptor.documents.length) throw new Error("Need the object ID to remove its previuous value");
    let removeOp = new CQL.RemoveOperation({
        action: myTypes.action.remove,
        opID: opDescriptor.opID,
        collections: opDescriptor.collections,
        documents: opDescriptor.documents,
        tableOptions: {secondaryTable:true},
        secondaryInfos: where
    })
    return removeOp;
}

export async function getPreviousValue(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.EncObject | null>
{
    if( opDescriptor.collections.length !== opDescriptor.documents.length) throw new Error("Need the object ID to check its previuous value");
    let getter:CQL.GetOperation = new CQL.GetOperation({
        action: myTypes.action.get,
        opID: opDescriptor.opID,
        collections: opDescriptor.collections,
        documents: opDescriptor.documents,
        tableOptions: {secondaryTable: false}
    });
    let DBanswer = await getter.execute();
    if(DBanswer.queryResults === undefined || DBanswer.queryResults.allResultsEnc === undefined || DBanswer.queryResults.allResultsEnc.length === 0) return null;
    return JSON.parse(DBanswer.queryResults.allResultsEnc[0].object);
}