import * as myTypes from "../../services/utils/myTypes";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { globalCounter } from "../../index";
import { Timer, RequestTracker } from "../../services/monitoring/Timer";
import { getPreviousValue, SaveMainView } from "../tedOperations/MainProjections";
import { getSaveSecondaryView, TStoCQLtypes, removePreviousValue, createSecondaryInfos } from "../tedOperations/SecondaryProjections";
import { tableCreationError, BaseOperation, BatchOperation } from "../../services/database/operations/baseOperations";
import { schedulingPolicy } from "cluster";

const noPreviousValue:Error = new Error("Unable to find a previous value");

export default async function saveRequest(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<BatchOperation>
{
    if(opDescriptor.clearObject === undefined && opDescriptor.encObject === undefined) throw new Error("missing object in save operation");
    if(opDescriptor.clearObject === undefined) opDescriptor.clearObject = myCrypto.decryptData(JSON.parse(opDescriptor.encObject as string), myCrypto.globalKey);
    globalCounter.inc("save_precompute");
    let timer = new Timer("save_precompute");

    let opArray:BaseOperation[] = [];
    try
    {
        console.log("getting previous value");
        let previousValueEnc = await getPreviousValue(opDescriptor);
        tracker?.endStep("secondary_table_read");

        if(previousValueEnc === null) throw noPreviousValue;
        let previousVersion:myTypes.ServerSideObject =  myCrypto.decryptData( previousValueEnc, myCrypto.globalKey);

        console.log("previous value = ", JSON.stringify(previousVersion));
        opDescriptor.clearObject = {...previousVersion, ...opDescriptor.clearObject};
        if(opDescriptor.schema !== undefined)
        {
            for(let key of opDescriptor.schema.dbSearchIndex)
            {
                if(opDescriptor.clearObject[key] !== undefined)
                {
                    let value = (opDescriptor.clearObject as myTypes.ServerSideObject)[key];
                    if(TStoCQLtypes.get(typeof(value)) !== undefined && previousVersion[key] !== value && previousVersion[key] !== undefined )
                    {
                        opArray.push(removePreviousValue(opDescriptor, createSecondaryInfos(previousVersion, key)));
                    }
                    opArray.push(getSaveSecondaryView(opDescriptor, key));
                }
            }
        }
        
    }
    catch(err)
    {   
        console.error(err);
        if((err === noPreviousValue || err.message.substr(0,18) === "unconfigured table")  )
        {
            if(opDescriptor.schema !== undefined)
            {
                for(let key of opDescriptor.schema.dbSearchIndex)
                {
                    if(opDescriptor.clearObject[key] !== undefined)
                    {
                        opArray.push(getSaveSecondaryView(opDescriptor, key));
                    }
                }
            }
        }
        else throw err;
    }
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    opArray.push(new SaveMainView(opDescriptor));
    timer.stop();
    return new BatchOperation(opArray, false);
}