import * as myTypes from "../../services/utils/myTypes";
import * as config from "../../../config/config";
import * as messageBroker from "../../services/messageBroker/MessageBrokerInterface";
import { GetTaskStore, RemoveTaskStore } from "../tedOperations/TaskStore";
import { runWriteOperation } from "./RequestHandling";
import { globalCounter } from "../../index";
import { Timer, RequestTracker } from "../../services/monitoring/Timer";
import { tableCreationError } from "../../services/database/operations/baseOperations";
import { delay, buildPath, processPath } from "../../services/utils/divers";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { GetMainView } from "../tedOperations/MainProjections";
import { sendToSocket } from "../../services/socket/sockectServer";
import { Organizations } from "aws-sdk";

export let mbInterface:messageBroker.TaskBroker|null;

export function setup():void
{
    switch(config.ted.broker)
    {
        case "RabbitMQ":
        {
            console.log("Intializing RabbitMQ interface");
            mbInterface = new messageBroker.RabbitMQBroker(config.rabbitmq.URL, config.rabbitmq.queueName, projectTask, config.rabbitmq.queueOptions, config.rabbitmq.messageOptions);
            break;
        }
        case "SQS":
        {
            console.log("Intializing SQS interface");
            mbInterface = new messageBroker.SQSBroker(projectTask, config.sqs.queueOptions, config.sqs.messageOptions);
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
        await runPendingOperation(op, false);
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
        keyOverride: {
            path: path,
        },
        options: {
            order: [{key: "op_id", order: "ASC"}],
            limit: config.ted.taskStoreBatchSize
        }
    })
    let result = await getOperation.execute();
    if( result.queryResults === undefined || result.queryResults.allResultsEnc === undefined) throw new Error("Unable to query pending operations on given path : " + path);
    return result.queryResults.allResultsEnc;
}

async function runPendingOperation(opLog:myTypes.DBentry, retry:boolean):Promise<void>
{
    try
    {
        let opDescriptor:myTypes.InternalOperationDescription = JSON.parse(opLog.object);
        let tracker = new RequestTracker("projection");
        await runWriteOperation(opDescriptor, tracker);
        sendToAfterSave(opDescriptor);
        tracker.updateLabel("stored_write_operation");
        tracker.endStep("cassandra_write");
        let rmDescriptor = {...opDescriptor};
        rmDescriptor.keyOverride = {
            path: opLog.path,
            op_id: opLog.op_id
        }
        let removeOperation = new RemoveTaskStore(rmDescriptor);
        await removeOperation.execute();
        tracker.endStep("taskstore_remove");
        tracker.log();
    }
    catch(err)
    {
        if(err === tableCreationError && retry)
        {
            await delay(10000);
            return runPendingOperation(opLog, true);
        }
        else throw err;
    }
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
            await runPendingOperation(op, true);
        }
    }while(operationsToForward.length == config.ted.taskStoreBatchSize )    
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
        await mbInterface.pushTask(path, opDescriptor.opID);
    }
}

export async function sendToAfterSave(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    return new Promise(async (resolve, reject) => 
    {
        try{
            console.log("ICIIIII", opDescriptor);
            if(opDescriptor.afterSave === undefined) resolve();
            else
            {
                console.log("LAAAAAAAA");
                let getOp = new GetMainView({
                    action: myTypes.action.get,
                    opID: opDescriptor.opID,
                    collections: opDescriptor.collections,
                    documents: opDescriptor.documents
                });
                let res = await getOp.execute()
                myCrypto.decryptResult(res, myCrypto.globalKey);
                console.log(res.queryResults);
                if(res.queryResults?.allResultsClear === undefined) throw new Error("Unable to find the created object");
                let ans:myTypes.ServerSideObject = {};
                ans["object"] = res.queryResults.allResultsClear[0].object as myTypes.ServerSideObject;
                ans["path"] = buildPath(opDescriptor.collections, opDescriptor.documents, false);
                ans["originalRequest"] = opDescriptor.afterSave.originalRequest;
                await sendToSocket("afterSave", ans, opDescriptor.afterSave);
                resolve();
            }
        }
        catch(err){
            reject(err);
        }        
    });
}