import * as myTypes from "../../services/utils/myTypes";
import saveRoutine from "./SaveRoutine";
import removeRoutine from "./RemoveRoutine";
import getRoutine, { EmptyResultError, fullsearchRequest } from "./GetRoutine";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { SaveTaskStore } from "../tedOperations/TaskStore";
import {
  BatchOperation,
  tableCreationError,
} from "../../services/database/operations/baseOperations";
import { mbInterface } from "./StoredTaskHandling";
import { v1 as uuidv1 } from "uuid";
import { Timer } from "../../services/monitoring/Timer";
import { processPath, delay, truncatePath } from "../../services/utils/divers";
import { SaveEventStore } from "../tedOperations/EventsTable";
import { GetMainView } from "../tedOperations/MainProjections";
import config from "../../services/configuration/configuration";

export function getInternalOperationDescription(
  request: myTypes.ServerRequestBody,
  path: string,
  afterTask?: boolean
): myTypes.InternalOperationDescription {
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
  let opDescriptor: myTypes.InternalOperationDescription = getInternalOperationDescription(
    request,
    path,
    afterTask
  );
  controlRequest(opDescriptor);
  myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
  try {
    switch (opDescriptor.action) {
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
    if (err === EmptyResultError)
      return { status: "Success", queryResults: { resultCount: 0 } };
    return { status: "Error", error: err };
  }
}

export async function logEvent(
  opDescriptor: myTypes.InternalOperationDescription,
  timer?: Timer
): Promise<void> {
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
    if(opDescriptor.action !== myTypes.action.get) throw new Error("This is not an authorized read operation");
    if((opDescriptor.options as myTypes.GetOptions).fullsearch === undefined)
    {
        let op = await getRoutine(opDescriptor);
        let res = await op.execute();
        return res;
    }
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
