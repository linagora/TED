import * as myTypes from "../BaseTools/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine from "./GetRoutine";
import * as OperationLog from "../TEDOperations/EventsTable";
import * as myCrypto from "./../BaseTools/CryptographicTools";
import { SaveTaskStore } from "../TEDOperations/TaskStore";
import { BatchOperation, tableCreationError } from "../BaseTools/BaseOperations";
import { mbInterface } from "./StoredTaskHandling";
import { v1 as uuidv1 } from "uuid";
import { Timer, RequestTracker } from "./../Monitoring/Timer";



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

export function buildPath(collections:string[], documents:string[], truncate:boolean):string
{
    if(collections.length - documents.length > 1 || collections.length - documents.length < 0) throw new Error("Invalid documents[] and collections[] length");
    let res:string[] = [];
    for(let i:number = 0; i < documents.length; i++)
    {
        res.push(collections[i]);
        res.push(documents[i]);
    }
    if(collections.length - documents.length === 1) res.push(collections.slice(-1)[0]);
    else if(truncate) res.pop();
    return res.join("/");
}

export function truncatePath(path:string):string
{
    let list:string[] = path.split("/");
    if(list.length % 2 === 0 ) list = list.slice(0,-1);
    return list.join("/");
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

async function delay(ms:number):Promise<void>
{
    return new Promise( resolve => setTimeout(resolve, ms) );
}

export default async function handleRequest(request:myTypes.ServerBaseRequest, tracker?:RequestTracker ):Promise<myTypes.ServerAnswer>
{
    let opDescriptor:myTypes.InternalOperationDescription = getInternalOperationDescription(request);
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    tracker?.endStep("encryption");
    switch(opDescriptor.action)
    {
        case myTypes.action.save:
        case myTypes.action.remove:
        {
            tracker?.updateLabel("taskstore_write")
            await logEvent(opDescriptor, tracker);
            tracker?.endStep("taskstore_write");
            if(mbInterface !== null) await mbInterface.pushTask(truncatePath(request.path), opDescriptor.opID);
            tracker?.endStep("broker_write");
            tracker?.log();
            return {status: "Success"};
        }
        case myTypes.action.get:
        {
            let res = await runReadOperation(opDescriptor, tracker);
            tracker?.endStep("cassandra_read")
            myCrypto.decryptResult(res, myCrypto.globalKey);
            tracker?.endStep("decryption");
            tracker?.log()
            return res;
        }
        default:
        {
            throw new Error("Unauthorized operation");
        }
    }    
}

export async function logEvent(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<void>
{
    let opWrite = new BatchOperation([new OperationLog.OperationLog(opDescriptor), new SaveTaskStore(opDescriptor)], tracker);
    try
    {
        await opWrite.execute();
    }
    catch(err)
    {
        if(err === tableCreationError)
        {
            await delay(1000);
            await logEvent(opDescriptor, tracker);
        }
        else throw err;
    }

}

export async function runWriteOperation(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<void>
{
    switch(opDescriptor.action)
    {
        case myTypes.action.save:
        {
            tracker?.updateLabel("save_operation");
            let op = await saveRoutine(opDescriptor, tracker);
            tracker?.endStep("operation_computation")
            await op.execute();
            break;
        }
        case myTypes.action.remove:
        {
            tracker?.updateLabel("remove_operation");
            let op = await removeRoutine(opDescriptor, tracker);
            tracker?.endStep("operation_computation")
            await op.execute();
            break;
        }
        default:
        {
            throw new Error("This is not an authorized wirte operation");
        }
    }
}

async function runReadOperation(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<myTypes.ServerAnswer>
{
    if(opDescriptor.action !== myTypes.action.get) throw new Error("This is not an authorized read operation");
    tracker?.updateLabel("get_operation");
    let op = await getRoutine(opDescriptor, tracker);
    tracker?.endStep("operation_computation");
    let res = await op.execute();
    return res;
}
