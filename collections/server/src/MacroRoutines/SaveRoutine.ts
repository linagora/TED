import * as myTypes from "./../BaseTools/myTypes";
import * as myCrypto from "./../BaseTools/CryptographicTools";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";
import { getPreviousValue, SaveMainView } from "../TEDOperations/MainProjections";
import { getSaveSecondaryView, TStoCQLtypes, removePreviousValue, createSecondaryInfos } from "../TEDOperations/SecondaryProjections";
import { tableCreationError, BaseOperation, BatchOperation } from "../BaseTools/BaseOperations";

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
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            if(TStoCQLtypes.get(typeof(value)) !== undefined && previousVersion[key] !== value && previousVersion[key] !== undefined )
            {
                opArray.push(removePreviousValue(opDescriptor, createSecondaryInfos(previousVersion, key)));
            }
            opArray.push(getSaveSecondaryView(opDescriptor, key));
        });
    }
    catch(err)
    {   
        console.error(err);
        if(err === noPreviousValue || err.message.substr(0,18) === "unconfigured table")
        {
            Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
            {
                if(TStoCQLtypes.get(typeof(value)) !== undefined)
                {
                    opArray.push(getSaveSecondaryView(opDescriptor, key));
                }
            });
        }
        else throw err;
    }
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    opArray.push(new SaveMainView(opDescriptor));
    timer.stop();
    return new BatchOperation(opArray, false);
}