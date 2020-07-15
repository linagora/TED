import * as myTypes from "../BaseTools/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine from "./GetRoutine";
import * as OperationLog from "./../BaseTools/OperationsTable";
import * as myCrypto from "./../BaseTools/CryptographicTools";
import { pushPending } from "../BaseTools/RedisTools";
import { SaveTaskStore } from "./../BaseTools/TaskStore";
import { BatchOperation } from "../BaseTools/BaseOperations";
import { v1 as uuidv1 } from "uuid";


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

export function buildPath(collections:string[], documents:string[]):string
{
    if(collections.length - documents.length > 1 || collections.length - documents.length < 0) throw new Error("Invalid documents[] and collections[] length");
    let path = "";
    for(let i:number = 0; i <documents.length; i++)
    {
        path = path + collections[i] + "/" + documents[i] + "/";
    }
    if(collections.length - documents.length === 1) path = path + collections.slice(-1)[0];
    else path = path.slice(0,-1);
    return path;
}

export async function createOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.Operation>
{
    try
    {
        if((opDescriptor.collections === [] || opDescriptor.documents === []) && opDescriptor.action !== myTypes.action.batch) throw new Error("missing field path in request");
        switch(opDescriptor.action)
        {
            case myTypes.action.save:
            {
                if(opDescriptor.encObject == undefined) throw new Error("Encrypted object not created in save request");
                return saveRoutine(opDescriptor);
            }
            case myTypes.action.get:
            {
                return getRoutine(opDescriptor);
            }
            case myTypes.action.remove:
            {
                return removeRoutine(opDescriptor);
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

export function getInternalOperationDescription(request:myTypes.ServerBaseRequest):myTypes.InternalOperationDescription
{
    let processedPath = processPath(request.path);
    let opDescriptor:myTypes.InternalOperationDescription = {
        action: request.action,
        opID: uuidv1(),
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

export default async function handleRequest(request:myTypes.ServerBaseRequest, ):Promise<myTypes.ServerAnswer>
{
    let opDescriptor:myTypes.InternalOperationDescription = getInternalOperationDescription(request);
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    switch(opDescriptor.action)
    {
        case myTypes.action.save:
        case myTypes.action.remove:
        {
            let opWrite = new BatchOperation([new OperationLog.OperationLog(opDescriptor), new SaveTaskStore(opDescriptor)]);
            await opWrite.execute();
            await pushPending(request.path);
            return {status: "Success"};
        }
        case myTypes.action.get:
        {
            let res = await runReadOperation(opDescriptor);
            myCrypto.decryptResult(res, myCrypto.globalKey);
            return res;
        }
        default:
        {
            throw new Error("Unauthorized operation");
        }
    }    
}

export async function runWriteOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    switch(opDescriptor.action)
    {
        case myTypes.action.save:
        {
            await (await saveRoutine(opDescriptor)).execute();
            break;
        }
        case myTypes.action.remove:
        {
            await (await removeRoutine(opDescriptor)).execute();
            break;
        }
        default:
        {
            throw new Error("This is not an authorized wirte operation");
        }
    }
}

async function runReadOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.ServerAnswer>
{
    if(opDescriptor.action !== myTypes.action.get) throw new Error("This is not an authorized read operation");
    return (await getRoutine(opDescriptor)).execute()
}
