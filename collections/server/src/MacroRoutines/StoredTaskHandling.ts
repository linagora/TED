import * as myTypes from "../BaseTools/myTypes";
import { peekPending, removePending } from "../BaseTools/RedisTools";
import { GetTaskStore, RemoveTaskStore } from "../BaseTools/TaskStore";
import { processPath, runWriteOperation, buildPath } from "./RequestHandling";
import { taskStoreBatchSize } from "../Config/config";
import Redis from "redis";

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
            limit: taskStoreBatchSize
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
    let message = await peekPending();
    if(message === null) 
    {
        return;
    }
    try
    {
        let operations:myTypes.DBentry[] = await getPendingOperations(message.message);
        console.log(operations.length + " pending operations found\n==================");
        for(let op of operations)
        {
            await runPendingOperation(op);
        }
        await removePending(message.id);
    }
    catch(err)
    {
        console.log("failed to run operation : " + err)
    }
}

export async function forwardCollection(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    let path:string = buildPath(opDescriptor.collections, opDescriptor.documents.slice(0,opDescriptor.collections.length - 1));
    console.log("collection to forward", path);
    let operationsToForward:myTypes.DBentry[] = await getPendingOperations(path);
    for(let op of operationsToForward)
    {
        await runPendingOperation(op);
    }
}

export async function RedisLoop():Promise<void>
{
    let subscriber = new Redis.RedisClient({});
    //subscriber.subscribe(ns+":rt:"+queueName);
    subscriber.on("message", (channel,message) => 
    {
        console.log("messages unread : ", message);
        for(let i:number = 0 ; i < parseInt(message); i++)
        {
            runProjectionTask();
        }
    });
}

async function getAllOperations():Promise<myTypes.DBentry[]>
{
    let getOperation = new GetTaskStore({
        action: myTypes.action.get,
        opID: "null",
        collections: [],
        documents: [],
        tableOptions: {
            secondaryTable: false,
            tableName: "global_taskstore"
        },
        keyOverride: {}
    })
    let result = await getOperation.execute();
    if( result.queryResults === undefined || result.queryResults.allResultsEnc === undefined) throw new Error("Unable to query all pending operations");
    return result.queryResults.allResultsEnc;
}

export async function fastForwardTaskStore():Promise<void>
{
    let allOps = await getAllOperations();
    for(let op of allOps)
    {
        await runPendingOperation(op);
    }
}
