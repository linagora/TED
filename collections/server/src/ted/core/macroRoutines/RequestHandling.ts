import * as myTypes from "../../services/utils/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine from "./GetRoutine";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { SaveTaskStore } from "../tedOperations/TaskStore";
import { BatchOperation, tableCreationError } from "../../services/database/operations/baseOperations";
import { mbInterface } from "./StoredTaskHandling";
import { v1 as uuidv1 } from "uuid";
import { Timer, RequestTracker } from "../../services/monitoring/Timer";
import { processPath, delay, truncatePath } from "../../services/utils/divers";
import { SaveEventStore } from "../tedOperations/EventsTable";

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

export function getInternalOperationDescription(request:myTypes.ServerRequestBody, path:string, afterSaveInfos?:myTypes.AfterSaveInfos):myTypes.InternalOperationDescription
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
        secondaryInfos: request.where === undefined ? undefined : {
            secondaryKey: request.where.field,
            secondaryValue: request.where.value,
            operator: request.where.operator
        },
        afterSave:afterSaveInfos
    }
    return opDescriptor;
}

export default async function handleRequest(request:myTypes.ServerRequestBody, path:string, afterSaveInfos?:myTypes.AfterSaveInfos, tracker?:RequestTracker ):Promise<myTypes.ServerAnswer>
{
    let opDescriptor:myTypes.InternalOperationDescription = getInternalOperationDescription(request, path, afterSaveInfos);
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    tracker?.endStep("encryption");
    switch(opDescriptor.action)
    {
        case myTypes.action.save:
        case myTypes.action.remove:
        {
            let totalResponseTime = new Timer("write_request");
            tracker?.updateLabel("taskstore_write")
            await logEvent(opDescriptor, tracker);
            tracker?.endStep("taskstore_write");
            if(mbInterface !== null) await mbInterface.pushTask(truncatePath(path), opDescriptor.opID);
            tracker?.endStep("broker_write");
            tracker?.log();
            totalResponseTime.stop();
            return {status: "Success"};
        }
        case myTypes.action.get:
        {
            let totalResponseTime = new Timer("read_request");
            tracker?.updateLabel("read_operation");
            let res = await runReadOperation(opDescriptor, tracker);
            tracker?.endStep("cassandra_read")
            myCrypto.decryptResult(res, myCrypto.globalKey);
            tracker?.endStep("decryption");
            tracker?.log();
            totalResponseTime.stop();
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
    console.log(opDescriptor);
    let opWrite = new BatchOperation([new SaveEventStore(opDescriptor), new SaveTaskStore(opDescriptor)], true);
    try
    {
        await opWrite.execute();
    }
    catch(err)
    {
        if(err === tableCreationError)
        {
            await delay(1000);
            return logEvent(opDescriptor, tracker);
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
            tracker?.endStep("cassandra_write");
            break;
        }
        case myTypes.action.remove:
        {
            tracker?.updateLabel("remove_operation");
            let op = await removeRoutine(opDescriptor, tracker);
            tracker?.endStep("operation_computation")
            await op.execute();
            tracker?.endStep("cassandra_write");
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
