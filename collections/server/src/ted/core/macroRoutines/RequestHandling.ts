import * as myTypes from "../../services/utils/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine, { EmptyResultError, fullsearchRequest } from "./GetRoutine";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { SaveTaskStore } from "../tedOperations/TaskStore";
import {
  BatchOperation,
  tableCreationError,
  GetOperation,
} from "../../services/database/operations/baseOperations";
import { mbInterface } from "./StoredTaskHandling";
import { v1 as uuidv1 } from "uuid";
import { Timer } from "../../services/monitoring/Timer";
import { processPath, delay, truncatePath } from "../../services/utils/divers";
import { SaveEventStore } from "../tedOperations/EventsTable";
import { GetMainView } from "../tedOperations/MainProjections";
import config from "../../services/configuration/configuration";
import { isUndefined } from "lodash";
import { resolve } from "path";


export function getInternalOperationDescription(
  request: myTypes.ServerRequestBody,
  path: string,
  afterTask?: boolean
): myTypes.InternalOperationDescription {
  /**
   * Builds an operation description from an external request.
   * 
   * Receives an external request an builds all the options, key and parameters required to run an operation on the DB.
   * 
   * @param {myTypes.ServerRequestBody} request the external request body.
   * @param {string} path the path of the operation.
   * @param {boolean} [afterTask] wether the operation needs an afterTask.
   * 
   * @returns {myTypes.InternalOperationDescription} the operation description in a standard format.
   */

  let processedPath = processPath(path);
  let opDescriptor: myTypes.InternalOperationDescription = {
    action: request.action,
    opID: uuidv1(),
    documents: processedPath.documents,
    collections: processedPath.collections,
    clearObject: request.object,
    options: {
        fullsearch:request.fullsearch,
        limit:request.limit,
        order:request.order,
        pageToken:request.pageToken,
        ttl:request.ttl,
    },
    schema: request.schema,
    secondaryInfos:
      request.where === undefined
        ? undefined
        : {
            secondaryKey: request.where.key,
            secondaryValue: request.where.value,
            operator: request.where.operator,
          },
    afterTask: afterTask,
  };
  console.log("\n ========== New request ==========\n", opDescriptor.action, " operation on ", path);
  return opDescriptor;
}

export default async function handleRequest(
  request: myTypes.ServerRequestBody,
  path: string,
  afterTask?: boolean
): Promise<myTypes.ServerAnswer> {
  /**
   * Handles an external request.
   * 
   * Runs the appropriated routine according to the request, and sends back the result.
   * 
   * @param {myTypes.ServerRequestBody} request the external request body.
   * @param {string} path the path of the operation.
   * @param {boolean} [afterTask] wether the operation needs an afterTask.
   * 
   * @returns {myTypes.ServerAnswer} the answer to send back.
   */

  //Builds the operation description
  let opDescriptor: myTypes.InternalOperationDescription = getInternalOperationDescription(
    request,
    path,
    afterTask
  );

  //Controls that the operation matches the requirements
  controlRequest(opDescriptor);
  
  //Encrypt the operation content (for save operations)
  myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);

  try {
    switch (opDescriptor.action) {

      //Case 1 : save or remove operation
        // => writes the operation to the TaskStore and pushes it to the MQ
      case myTypes.action.save:
      case myTypes.action.remove: {
        let logTimer = new Timer("write_request");
        await logEvent(opDescriptor);
        logTimer.stop();
        let timer = new Timer("mb_write");
        if (mbInterface !== null)
          await mbInterface.pushTask(truncatePath(path), opDescriptor.opID);
        timer.stop();
        console.log("Operation logged and added to the MQ");
        return { status: "Success" };
      }
      //Case 2 : get operation
        // => runs the operation and returns the result.
      case myTypes.action.get: {
        let totalResponseTime = new Timer("read_request");
        let res = await runReadOperation(opDescriptor);
        myCrypto.decryptResult(res, myCrypto.globalKey);
        totalResponseTime.stop();
        return res;
      }
      default: {
        throw new Error("Unauthorized operation");
      }
    }
  } catch (err) {

    //Error triggered when a get operation finds no result
    if (err === EmptyResultError)
      return { status: "Success", queryResults: { resultCount: 0 } };
    return { status: "Error", error: err };
  }
}

export async function logEvent(
  opDescriptor: myTypes.InternalOperationDescription,
  timer?: Timer
): Promise<void> {
  /**
   * Writes an operation to the TaskStore and to the EventStore.
   * 
   * Simultaneously writes the operation on both tables. If the database core permits it, both operations are made in isolation to make sure the DB remains coherent.
   * 
   * @param {myTypes.InternalOperationDescription} opDescriptor the operation to log.
   * @param {Timer} [timer] optionnal timer when the function is called recursively.
   * 
   * @returns {Promise<void>} Resolves when the operation is added to both tables.
   */
  
  if (timer === undefined) timer = new Timer("taskstore_write");
  let enableIsolation = config.configuration.ted.dbCore !== "keyspace";
  let opWrite = new BatchOperation(
    [new SaveEventStore(opDescriptor), new SaveTaskStore(opDescriptor)],
    enableIsolation
  );
  try {
    await opWrite.execute();
    timer.stop();
  } 
  catch (err) {
    console.error(err);
    //If the table doesn't exist retry after a delay (for Amazon Keyspace)
    if (err === tableCreationError) {
      await delay(1000);
      return logEvent(opDescriptor, timer);
    } else {
      timer.stop();
      throw err;
    }
  }
}

export async function runWriteOperation(
  opDescriptor: myTypes.InternalOperationDescription
): Promise<void> {
  /**
   * Runs a save or remove operation from an operation description.
   * 
   * Computes all the secondary operations triggered by the given operation, and runs them all.
   * 
   * @param {myTypes.InternalOperationDescription} opDescriptor the operation description.
   * 
   * @returns {Promise<void>} Resolve when all the modifications have been applied on the database.
   */

  let timer = new Timer("projection_write");
  try {
    switch (opDescriptor.action) {
      case myTypes.action.save: {
        let op = await saveRoutine(opDescriptor);
        await op.execute();
        timer.stop();
        break;
      }
      case myTypes.action.remove: {
        let op = await removeRoutine(opDescriptor);
        await op.execute();
        timer.stop();
        break;
      }
      default: {
        timer.stop();
        throw new Error("This is not an authorized wirte operation");
      }
    }
  } catch (err) {
    timer.stop();
    throw err;
  }
}

async function runReadOperation(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.ServerAnswer>
{
  /**
   * Runs a request on the database and returns its result.
   * 
   * Computes all the secondary request to ask to the DB and returns the result of the given request.
   * 
   * @param {myTypes.InternalOperationDescription} opDescriptor the request description.
   * 
   * @returns {myTypes.ServerAnswer} the result of the request.
   */

  if(opDescriptor.action !== myTypes.action.get) throw new Error("This is not an authorized read operation");
  //Case 1 : standard query => GetRoutine
  if((opDescriptor.options as myTypes.GetOptions).fullsearch === undefined)
  {
    let ops = await getRoutine(opDescriptor);
    if(ops instanceof GetOperation)
    {
      let res = await ops.execute();
      return res;
    }
    else
    {
      let encRes:myTypes.ServerSideObject[] = [];
      let pageToken: string|undefined = undefined;
      for(let op of ops)
      {
        let res = await op.execute();
        if(res.queryResults !== undefined && res.queryResults.allResultsEnc !== undefined)
        encRes = encRes.concat(res.queryResults.allResultsEnc);
        if(res.queryResults !== undefined && res.queryResults.pageToken !== undefined)
          pageToken = res.queryResults.pageToken;
      }
      return {
        status: "success",
        queryResults: {
          resultCount: encRes.length,
          allResultsEnc: encRes,
          pageToken: pageToken,
        }
      };
    }
  }
  //Case 2 : fullsearch query => custom routine
  else
  {
    let res:myTypes.ServerSideObject[] = [];
    let getOps:GetMainView[] = await fullsearchRequest(opDescriptor);
    for(let op of getOps)
  {
      let ans = await op.execute();
      if(ans.queryResults !== undefined && ans.queryResults.allResultsEnc !== undefined)
      {
        res = res.concat(ans.queryResults.allResultsEnc);
      }
    }
    return {
      status: "success",
      queryResults: {
        resultCount: res.length,
        allResultsEnc: res,
        }
    };
  }
}

function controlRequest(
  opDescriptor: myTypes.InternalOperationDescription
): void {
  /**
   * Controls that the operation description is correctly built.
   * 
   * Checks for incompatibilities between the parameters of the operation descritption.
   * 
   * @param {myTypes.InternalOperationDescription} opDescriptor the operation to check.
   * 
   * @returns {void} throws an error according to the issue found.
   */
  
  switch (opDescriptor.action) {
    case myTypes.action.save: {
      if (opDescriptor.clearObject === undefined)
        throw new Error("missing object in save operation");
      if (opDescriptor.documents.length !== opDescriptor.collections.length)
        throw new Error("Invalid path for save operation");
      break;
    }
    case myTypes.action.get: {
      break;
    }
    case myTypes.action.remove: {
      if (opDescriptor.collections.length !== opDescriptor.documents.length)
        throw new Error("Collection delete not yet implemented");
      break;
    }
  }
}
