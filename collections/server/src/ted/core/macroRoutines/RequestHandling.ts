import * as myTypes from "../../services/utils/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine from "./GetRoutine";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { SaveTaskStore } from "../tedOperations/TaskStore";
import { BatchOperation, tableCreationError } from "../../services/database/operations/baseOperations";
import { mbInterface } from "./StoredTaskHandling";
import { v1 as uuidv1 } from "uuid";
import { Timer } from "../../services/monitoring/Timer";
import { processPath, delay, truncatePath } from "../../services/utils/divers";
import { SaveEventStore } from "../tedOperations/EventsTable";
import * as config from "../../../config/config";

export async function createOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.GenericOperation>
{
    try
    {
        //if((opDescriptor.collections === [] || opDescriptor.documents === []) && opDescriptor.action !== myTypes.action.batch) throw new Error("missing field path in request");
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

export function getInternalOperationDescription(request:myTypes.ServerRequestBody, path:string, afterTask?:boolean):myTypes.InternalOperationDescription
{
    console.log(request);
    let processedPath = processPath(path);
    let opDescriptor:myTypes.InternalOperationDescription = {
        action: request.action,
        opID: uuidv1(),
        documents: processedPath.documents,
        collections: processedPath.collections,
        clearObject: request.object,
        options: request.options,
        schema: request.schema,
        secondaryInfos: request.where === undefined ? undefined : {
            secondaryKey: request.where.field,
            secondaryValue: request.where.value,
            operator: request.where.operator
        },
        afterTask: afterTask
    }
    return opDescriptor;
}

export default async function handleRequest(request:myTypes.ServerRequestBody, path:string, afterTask?:boolean):Promise<myTypes.ServerAnswer>
{
    let opDescriptor:myTypes.InternalOperationDescription = getInternalOperationDescription(request, path, afterTask);
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    try
    {
        switch(opDescriptor.action)
        {
            case myTypes.action.save:
            case myTypes.action.remove:
            {
                let totalResponseTime = new Timer("write_request");
                await logEvent(opDescriptor);
                let timer = new Timer("mb_write");
                if(mbInterface !== null) await mbInterface.pushTask(truncatePath(path), opDescriptor.opID);
                timer.stop();
                totalResponseTime.stop();
                return {status: "Success"};
            }
            case myTypes.action.get:
            {
                let totalResponseTime = new Timer("read_request");
                let res = await runReadOperation(opDescriptor);
                myCrypto.decryptResult(res, myCrypto.globalKey);
                totalResponseTime.stop();
                return res;
            }
            default:
            {
                throw new Error("Unauthorized operation");
            }
        } 
    }
    catch(err)
    {
        return {status : "Error", error: err};
    }
}

export async function logEvent(opDescriptor:myTypes.InternalOperationDescription, timer?:Timer):Promise<void>
{
    if(timer === undefined) timer = new Timer("taskstore_write");
    let enableIsolation = config.cassandra.core !== "keyspace";
    let opWrite = new BatchOperation([new SaveEventStore(opDescriptor), new SaveTaskStore(opDescriptor)], enableIsolation);
    try
    {
        await opWrite.execute();
        timer.stop();
    }
    catch(err)
    {
        if(err === tableCreationError)
        {
            await delay(1000);
            return logEvent(opDescriptor, timer);
        }
        else
        {
            timer.stop();
            throw err;
        }
    }

}

export async function runWriteOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    let timer = new Timer("projection_write");
    try
    {
        switch(opDescriptor.action)
        {
            case myTypes.action.save:
            {
                let op = await saveRoutine(opDescriptor);
                await op.execute();
                timer.stop();
                break;
            }
            case myTypes.action.remove:
            {
                let op = await removeRoutine(opDescriptor);
                await op.execute();
                timer.stop()
                break;
            }
            default:
            {
                timer.stop();
                throw new Error("This is not an authorized wirte operation");
            }
        }
    }
    catch(err)
    {
        timer.stop();
        throw err;
    }
}

async function runReadOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.ServerAnswer>
{
    if(opDescriptor.action !== myTypes.action.get) throw new Error("This is not an authorized read operation");
    let op = await getRoutine(opDescriptor);
    let res = await op.execute();
    return res;
}
