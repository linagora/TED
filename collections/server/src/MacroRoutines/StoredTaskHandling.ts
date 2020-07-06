import * as myTypes from "../BaseTools/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine from "./GetRoutine";
import * as OperationLog from "../BaseTools/OperationsTable";
import { key } from "../index";
import * as myCrypto from "../BaseTools/CryptographicTools";
import { pushOperation, popOperation } from "../BaseTools/RedisTools";
import { SaveTaskStore, GetTaskStore, RemoveTaskStore } from "../BaseTools/TaskStore";
import * as CQL from "../BaseTools/BaseOperations";
import { v1 as uuidv1 } from "uuid";
import { processPath, runWriteOperation } from "./RequestHandling";
import { KeyObject } from "crypto";
import { pathToFileURL } from "url";

async function getPendingOperations(path:string):Promise<myTypes.DBentry[]>
{
    let processedPath = processPath(path);
    let getOperation = new GetTaskStore({
        action: myTypes.action.get,
        opID: "null",
        collections: processedPath.collections,
        documents: processedPath.documents,
        tableOptions: {
            secondaryTable: false,
            tableName: "global_taskstore"
        },
        keyOverride: {
            path: path,
        },
        options: {
            order: "op_id ASC",
            limit: 20
        }
    })
    let result = await getOperation.execute();
    if( result.queryResults === undefined || result.queryResults.allResultsEnc === undefined) throw new Error("Unable to query pending operations on given path : " + path);
    return result.queryResults.allResultsEnc;
}

async function runPendingOperation(opLog:myTypes.DBentry):Promise<void>
{
    let opDescriptor:myTypes.InternalOperationDescription = JSON.parse(opLog.object);
    await runWriteOperation(opDescriptor);
    let rmDescriptor = {...opDescriptor};
    rmDescriptor.keyOverride = {
        path: opLog.path,
        op_id: opLog.op_id
    }
    let removeOperation = new RemoveTaskStore(rmDescriptor);
    await removeOperation.execute();
}

async function runProjectionTask():Promise<void>
{
    let path = await popOperation();
    if(path === null) 
    {
        console.log("No pending operation");
        return;
    }
    try
    {
        let operations:myTypes.DBentry[] = await getPendingOperations(path);
        console.log(operations.length + " pending operations found\n==================");
        for(let op of operations)
        {
            await runPendingOperation(op);
        }
    }
    catch(err)
    {
        console.log("failed to run operation : " + err)
        pushOperation(path)
    }
}

function delay(ms: number) {
    return new Promise( resolve => setTimeout(resolve, ms) );
}

export async function RedisLoop():Promise<void>
{
    while(1)
    {
        await delay(5000);
        await runProjectionTask();
    }
}
