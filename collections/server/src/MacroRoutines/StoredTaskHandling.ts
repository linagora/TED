import * as myTypes from "../BaseTools/myTypes";
import * as config from "./../Config/config";
import * as messageBroker from "./../MessageBroker/MessageBrokerInterface";
import { GetTaskStore, RemoveTaskStore } from "../BaseTools/TaskStore";
import { processPath, runWriteOperation, buildPath } from "./RequestHandling";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";

export let mbInterface:messageBroker.TaskBroker|null;

export function setup():void
{
    switch(config.broker)
    {
        case "RabbitMQ":
        {
            console.log("Intializing RabbitMQ interface");
            mbInterface = new messageBroker.RabbitMQBroker(config.amqpURL, config.amqpQueueName, projectTask, config.amqpQueueOptions, config.amqpMessageOptions);
            break;
        }
        default:
        {
            console.log("No valid broker detected in config file");
            mbInterface = null;
            break;
        }
    }
  
}

export async function projectTask(path:string):Promise<void>
{
    let operations = await getPendingOperations(path);
    for(let op of operations)
    {
        await runPendingOperation(op);
    }
}

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
            limit: config.taskStoreBatchSize
        }
    })
    let result = await getOperation.execute();
    if( result.queryResults === undefined || result.queryResults.allResultsEnc === undefined) throw new Error("Unable to query pending operations on given path : " + path);
    return result.queryResults.allResultsEnc;
}

async function runPendingOperation(opLog:myTypes.DBentry):Promise<void>
{
    let opDescriptor:myTypes.InternalOperationDescription = JSON.parse(opLog.object);
    let tracker = new RequestTracker({
        action: myTypes.action.projection,
        path: buildPath(opDescriptor.collections, opDescriptor.documents, false),
    })
    await runWriteOperation(opDescriptor, tracker);
    tracker.endStep("cassandra write");
    let rmDescriptor = {...opDescriptor};
    rmDescriptor.keyOverride = {
        path: opLog.path,
        op_id: opLog.op_id
    }
    let removeOperation = new RemoveTaskStore(rmDescriptor);
    await removeOperation.execute();
    tracker.endStep("taskstore remove");
    tracker.log();
}


export async function forwardCollection(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    let path:string = buildPath(opDescriptor.collections, opDescriptor.documents, true);
    console.log("collection to forward", path);
    let operationsToForward:myTypes.DBentry[]
    do
    {
        operationsToForward = await getPendingOperations(path);
        for(let op of operationsToForward)
        {
            await runPendingOperation(op);
        }
    }while(operationsToForward.length == config.taskStoreBatchSize )    
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
    if(mbInterface === null)
    {
        console.log("Unable to fastforward operations without task broker. Operations will be executed on read.");
        return;
    }
    for(let op of allOps)
    {
        let opDescriptor:myTypes.InternalOperationDescription = JSON.parse(op.object);
        let path = buildPath(opDescriptor.collections, opDescriptor.documents, true)
        await mbInterface.pushTask(path);
        console.log("pushed op : ", path);
    }
}
