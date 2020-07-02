import * as CQL from "./BaseOperations";
import * as myTypes from "./myTypes";
import * as secondary from "./SecondaryIndexOperations";
import * as myCrypto from "./CryptographicTools";
import { key } from "./index";
import { KeyObject } from "crypto";
import { type } from "os";
import { rejects, match } from "assert";
import { domainToUnicode } from "url";
import { Interface } from "readline";

let nullUUID = "00000000-0000-0000-0000-000000000000";

export function processPath(path:string):{documents:string[], collections:string[]}
{
    if(path === undefined) return {documents:[], collections:[]};
    path = path.toLowerCase();
    let documents:string[] = [];
    let collections:string[] = [];
    let names:string[] = path.split('/');
    for(let i:number = 0; i<names.length; i++)
    {
        if(i%2 === 0)
        {
            collections.push(names[i]);
            let nameCtrl = names[i].match("^[a-z]*$");
            if(nameCtrl === null) throw new Error("Invalid collection name");
        } 
        else
        {
            documents.push(names[i]);
            let nameCtrl = names[i].match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
            if(nameCtrl === null) throw new Error("Invalid document ID");
        }      
    }
    return {documents:documents, collections:collections};
}

export async function createOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.Operation>
{
    try
    {
        if((opDescriptor.collections === [] || opDescriptor.documents === []) && opDescriptor.action !== myTypes.action.batch) throw new Error("missing field path in request");
        switch(opDescriptor.action)
        {
            case myTypes.action.batch:
            {
                if(opDescriptor.operations === undefined) throw new Error("missing field operations in batch request");
                let batch = new CQL.BatchOperation([]);
                for(let req of opDescriptor.operations)
                {
                    let operation = createOperation(req);
                    if(!(operation instanceof CQL.BaseOperation)) throw new Error("Only base operations are allowed in a batch");
                    batch.push(operation);
                }
                return batch;
            }
            case myTypes.action.save:
            {
                if(opDescriptor.encObject == undefined) throw new Error("Encrypted object not created in save request");
                let batch:CQL.BatchOperation = await saveRequest(opDescriptor);
                return batch;
            }
            case myTypes.action.get:
            {
                let op:Promise<CQL.BaseOperation> = getRequest(opDescriptor);
                return await op;
            }
            case myTypes.action.remove:
            {
                let batch:CQL.BatchOperation = removeRequest(opDescriptor);
                return batch;
            }
            default:
            {
                throw new Error("Unknown action in request");
            }
        }
    }
    catch(err)
    {
        console.log("Responsible opDescriptor =\n", opDescriptor);
        console.log("Failed to create operation: \n",err);
        throw err;
    }
};

async function saveRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<CQL.BatchOperation>
{
    if(opDescriptor.clearObject === undefined) throw new Error("missing object in save operation");
    let opArray:CQL.BaseOperation[] = [];
    opArray.push(new CQL.SaveOperation(opDescriptor));
    let checkOperation = new CQL.GetOperation({
        action: myTypes.action.get,
        collections: opDescriptor.collections,
        documents: opDescriptor.documents,
        tableOptions: {secondaryTable:false}
    })
    try
    {
        let previousVersion:myTypes.ServerSideObject = await checkOperation.execute()
        .then( (result:myTypes.ServerAnswer) => 
        {
            myCrypto.decryptResult(result, key);
            if(result.queryResults === undefined || result.queryResults.allResultsClear === undefined) return {};
            return result.queryResults.allResultsClear[0];
        });
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            console.log(value, "is of type : ", typeof(value));
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined && previousVersion[key] !== value)
            {
                opArray.push(secondary.removePreviousValue(opDescriptor, {field: key, value: previousVersion[key], operator: myTypes.Operator.eq}));
                opArray.push(secondary.getSaveOperationSecondaryIndex(opDescriptor, key));
            }
        });
    }
    catch
    {
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            console.log(value, "is of type : ", typeof(value));
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined)
            {
                opArray.push(secondary.getSaveOperationSecondaryIndex(opDescriptor, key));
            }
        });
    }
    return new CQL.BatchOperation(opArray);
}

async function getRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<CQL.GetOperation>
{
    if(opDescriptor.collections.length ===  opDescriptor.documents.length) return new CQL.GetOperation(opDescriptor);
    if(opDescriptor.secondaryInfos === undefined) return new CQL.GetOperation(opDescriptor);
    let secondaryOp = secondary.getGetOperationSecondaryIndex(opDescriptor);
    let objectKey:string = opDescriptor.collections.slice(-1)[0];
    let matchingIDs:string[] = await secondaryOp.execute()
    .then( (result:myTypes.ServerAnswer):string[] => 
    {
        let res:string[] = []
        if(result.queryResults === undefined || result.queryResults.allResultsClear === undefined) throw new Error("Unable to query requested fields : " + JSON.stringify(result));
        for(let object of result.queryResults.allResultsClear)
        {
            res.push(object[objectKey]);
        }
        return res;
    });
    console.log(matchingIDs);
    if(matchingIDs.length === 0) matchingIDs.push(nullUUID); //vraiment pas ouf, à modifier ASAP
    let op = new CQL.GetOperation({
        action: myTypes.action.get,
        collections: opDescriptor.collections,
        documents: opDescriptor.documents,
        tableOptions: {secondaryTable:false},
        secondaryInfos: {
            field: opDescriptor.collections.slice(-1)[0],
            operator: myTypes.Operator.in,
            value: matchingIDs
        }
    });
    console.log("final op =\n", op);
    return op;
}

function removeRequest(opDescriptor:myTypes.InternalOperationDescription):CQL.BatchOperation
{
    let opArray:CQL.BaseOperation[] = [];
    if(opDescriptor.collections.length === opDescriptor.documents.length)
    {
        opArray.push(new CQL.RemoveOperation(opDescriptor));
    }
    if(opDescriptor.clearObject !== undefined)
    {
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            console.log(value);
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined)
            {
                opArray.push(secondary.getRemoveOperationSecondaryIndex(opDescriptor));
            }
        });
    }
    return new CQL.BatchOperation(opArray);
}

export function getInternalOperationDescription(request:myTypes.ServerBaseRequest):myTypes.InternalOperationDescription
{
    let processedPath = processPath(request.path);
    let opDescriptor:myTypes.InternalOperationDescription = {
        action: request.action,
        documents: processedPath.documents,
        collections: processedPath.collections,
        clearObject: request.object,
        options: request.options,
        tableOptions:{secondaryTable:false},
        secondaryInfos: request.where
    }
    if(request.operations !== undefined)
    {
        opDescriptor.operations = [];
        for(let op of request.operations)
        {
            opDescriptor.operations.push(getInternalOperationDescription(op));
        }
    }
    return opDescriptor;
}