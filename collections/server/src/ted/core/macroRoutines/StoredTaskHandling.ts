import * as myTypes from "../../services/utils/myTypes";
import config from "../../services/configuration/configuration";
import * as messageBroker from "../../services/messageBroker/MessageBrokerInterface";
import { GetTaskStore, RemoveTaskStore } from "../tedOperations/TaskStore";
import { runWriteOperation } from "./RequestHandling";
import { Timer } from "../../services/monitoring/Timer";
import { tableCreationError } from "../../services/database/operations/baseOperations";
import { delay, buildPath, processPath } from "../../services/utils/divers";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { GetMainView } from "../tedOperations/MainProjections";


//MB interface used to store projection tasks.
export let mbInterface: messageBroker.TaskBroker | null;
//MB interface used to send afterTasks.
export let afterTaskSender: messageBroker.TaskBroker | null;

/**
 * Setup both of the message queues according to configuration informations.
 */
export function setup(): void {

  switch (config.configuration.ted.broker) {
    case "RabbitMQ": {
      console.log("Intializing RabbitMQ interface");

      mbInterface = new messageBroker.RabbitMQBroker(
        config.configuration.rabbitmq.url,
        config.configuration.rabbitmq.taskBroker.queueName,
        projectTask,
        config.configuration.rabbitmq.taskBroker.queueOptions,
        config.configuration.rabbitmq.taskBroker.messageOptions,
        config.configuration.rabbitmq.taskBroker.rejectionTimeout,
        config.configuration.rabbitmq.taskBroker.prefetchCount
      );

      afterTaskSender = new messageBroker.RabbitMQBroker(
        config.configuration.rabbitmq.url,
        config.configuration.rabbitmq.afterTaskBroker.queueName,
        dummyCallback,
        config.configuration.rabbitmq.afterTaskBroker.queueOptions,
        config.configuration.rabbitmq.afterTaskBroker.messageOptions,
        config.configuration.rabbitmq.afterTaskBroker.rejectionTimeout,
        config.configuration.rabbitmq.afterTaskBroker.prefetchCount
      );
      break;
    }
    case "SQS": {
      console.log("Intializing SQS interface");

      mbInterface = new messageBroker.SQSBroker(
        projectTask,
        config.configuration.sqs.taskBroker.queueOptions,
        config.configuration.sqs.taskBroker.messageOptions,
        config.configuration.sqs.taskBroker.prefetchCount,
        true
      );

      afterTaskSender = new messageBroker.SQSBroker(
        dummyCallback,
        config.configuration.sqs.afetrTaskBroker.queueOptions,
        config.configuration.sqs.afetrTaskBroker.messageOptions,
        0,
        false
      );
      break;
    }
    default: {
      console.log("No valid broker detected in config file");
      mbInterface = null;
      afterTaskSender = null;
      break;
    }
  }
}

/**
 * Callback for the projectionTasks interface.
 * 
 * When the projectionTasks interface gets a new message, this callback reads the TaskStore and runs the pending operations from the TaskStore.
 * 
 * @param {string} path The path to the collection that has pending operations.
 * 
 * @returns {Promise<void>} Resolves when the operations are done.
 */
export async function projectTask(path: string): Promise<void> {
  
  let operations = await getPendingOperations(path);
  for (let op of operations) {
    await runPendingOperation(op, false);
  }
}

//Callback for the afterTasks interface (this interface never reads anything)
async function dummyCallback(path: string): Promise<void> {}

/**
 * Reads the TaskStore to get the pending operations on a specified collection.
 * 
 * Gets no more operations than specified in the configuration, only on the given collection. Then returns a raw format of the operations, as they were stored in the TaskStore.
 * 
 * @param {string} path The path to the collection on which apply the operations.
 * 
 * @returns {Promise<myTypes.DBentry[>]} An array of raw operations.
 */
async function getPendingOperations(path: string): Promise<myTypes.DBentry[]>
{

  let timer = new Timer("taskstore_read");
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
      order: { key: "op_id", order: "ASC" },
      limit: config.configuration.ted.taskStoreBatchSize,
    },
  });
  let result = await getOperation.execute();
  //console.log("pending operations : ",   result.queryResults?.allResultsEnc?.map( (value) => JSON.parse(value["object"]) ));

  if (
    result.queryResults === undefined ||
    result.queryResults.allResultsEnc === undefined
  )
    throw new Error(
      "Unable to query pending operations on given path : " + path
    );
  timer.stop();
  return result.queryResults.allResultsEnc;
}

/**
 * Runs a raw operation.
 * 
 * Runs an operation from a log stored on the TaskStore. Specifies if the operation needs to be retried in case of a table creation.
 * 
 * @param {myTypes.DBentry} opLog a log from the TaskStore.
 * @param {boolean} retry Whether the operation should be retried if the table doesn't exist.
 * 
 * @returns {Promise<void>} Resolves when the operation ends on the DB.
 */
async function runPendingOperation(
  opLog: myTypes.DBentry,
  retry: boolean
): Promise<void> {
  
  let timer = new Timer("projection");
  try {
    //Parses the log.
    let opDescriptor: myTypes.InternalOperationDescription = JSON.parse(
      opLog.object
    );

    //Runs the opeartion on the DB
    await runWriteOperation(opDescriptor);

    //Emits an afterTask if needed
    sendToAfterTask(opDescriptor);
    
    //Removes the operation from the TaskStore.
    let rmDescriptor = { ...opDescriptor };
    rmDescriptor.keyOverride = {
      path: opLog.path,
      op_id: opLog.op_id,
    };
    let removeOperation = new RemoveTaskStore(rmDescriptor);
    let rmTimer = new Timer("taskstore_remove");
    await removeOperation.execute();

    rmTimer.stop();
    timer.stop();
  } catch (err) {
    timer.stop();

    //If the table doesn't exist and the operation needs to be retried, wait and retry (for Amazon Keyspace)
    if (err === tableCreationError && retry) {
      await delay(10000);
      return runPendingOperation(opLog, true);
    } else throw err;
  }
}

/**
 * Runs all the pending operations on a collection.
 * 
 * Receives an operation description, reads and then runs all the pending operations on the collection concerned by the operation description.
 * 
 * @param {myTypes.InternalOperationDescription} opDescriptor The operation that needs a collection forwarding.
 * 
 * @returns {Promise<void>} Resolves when the collection is up to date.
 */
export async function forwardCollection(
  opDescriptor: myTypes.InternalOperationDescription
): Promise<void> {
  
  try {
    let timer = new Timer("collection_forwarding");

    let path: string = buildPath(
      opDescriptor.collections,
      opDescriptor.documents,
      true
    );
    console.log("collection to forward", path);
    let operationsToForward: myTypes.DBentry[];

    //While there are some pending operations, runs them.
    do {
      operationsToForward = await getPendingOperations(path);
      for (let op of operationsToForward) {
        await runPendingOperation(op, true);
      }
    } while (
      operationsToForward.length == config.configuration.ted.taskStoreBatchSize
    );
    timer.stop();
  } catch (err) {}
}

/**
 * Returns all the pending operations in the TaskStore. Used when recovering from a crash or rebooting.
 * 
 * @returns {Promise<myTypes.DBentry[]>} An array with the logs of all the pending operations.
 */
async function getAllOperations(): Promise<myTypes.DBentry[]> {
  
  let timer = new Timer("taskstore_read");
  let getOperation = new GetTaskStore({
    action: myTypes.action.get,
    opID: "null",
    collections: [],
    documents: [],
    tableOptions: {
      secondaryTable: false,
      tableName: "global_taskstore",
    },
    keyOverride: {},
  });
  let result = await getOperation.execute();
  timer.stop();
  if (
    result.queryResults === undefined ||
    result.queryResults.allResultsEnc === undefined
  )
    return [];
  return result.queryResults.allResultsEnc;
}

/**
 * Pushes all the pending operations in the TaskStore to th projectionTasks MQ. Used when recovering from a crash or rebooting.
 * 
 * @returns {Promise<void>} Resolves when the TaskStore is empty.
 */
export async function fastForwardTaskStore(): Promise<void> {
  
  let allOps = await getAllOperations();
  if (mbInterface === null) {
    console.log(
      "Unable to fastforward operations without task broker. Operations will be executed on read."
    );
    return;
  }
  for (let op of allOps) {
    let opDescriptor: myTypes.InternalOperationDescription = JSON.parse(
      op.object
    );
    let path = buildPath(
      opDescriptor.collections,
      opDescriptor.documents,
      true
    );
    await mbInterface.pushTask(path, opDescriptor.opID);
  }
}

/**
 * Pushes a task to the afterTasks MQ.
 * 
 * Once an operation is done, pushes the operation result to the afterTasks MQ according to the operation description.
 * 
 * @param {myTypes.InternalOperationDescription} opDescriptor The operation to push in the afterTask Queue.
 * 
 * @returns {Promise<void>} Resolves when the operation is pushed.
 */
export async function sendToAfterTask(
  opDescriptor: myTypes.InternalOperationDescription
): Promise<void> {
  
  return new Promise(async (resolve, reject) => {
    try {

      //Case 1 : the operation doesn't need an afterTask
      if (opDescriptor.afterTask == !true) resolve();

      else 
      {
        let getOp = new GetMainView({
          action: myTypes.action.get,
          opID: opDescriptor.opID,
          collections: opDescriptor.collections,
          documents: opDescriptor.documents,
        });
        let res = await getOp.execute();
        myCrypto.decryptResult(res, myCrypto.globalKey);

        //Case 2 : there is a result to send with the afterTask
        if (
          res.queryResults !== undefined &&
          res.queryResults?.resultCount > 0
        ) 
        {
          if (res.queryResults?.allResultsClear === undefined)
            throw new Error("Unable to find the created object");
          let ans: myTypes.AfterTask = {
            action: opDescriptor.action,
            path: buildPath(
              opDescriptor.collections,
              opDescriptor.documents,
              false
            ),
            object: res.queryResults.allResultsClear[0]
              .object as myTypes.ServerSideObject,
          };
          await afterTaskSender?.pushTask(
            JSON.stringify(ans),
            opDescriptor.opID
          );
          resolve();
        }

        //Case 3 : there is no object to return with the afterTask
        else 
        {
          let ans: myTypes.AfterTask = {
            action: opDescriptor.action,
            path: buildPath(
              opDescriptor.collections,
              opDescriptor.documents,
              false
            ),
            object: {},
          };
          await afterTaskSender?.pushTask(
            JSON.stringify(ans),
            opDescriptor.opID
          );
          resolve();
        }
      }
    } catch (err) {
      reject(err);
    }
  });
}
