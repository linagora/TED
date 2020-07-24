import * as myTypes from "./../BaseTools/myTypes";
import * as myCrypto from "./../BaseTools/CryptographicTools";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";
import { RemoveMainView, getPreviousValue } from "../TEDOperations/MainProjections";
import { BatchOperation, BaseOperation } from "../BaseTools/BaseOperations";
import { TStoCQLtypes, getRemoveSecondaryView, createSecondaryInfos } from "../TEDOperations/SecondaryProjections";

export default async function removeRequest(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<BatchOperation>
{
    globalCounter.inc("remove_precompute");
    let timer = new Timer("remove_precompute");
    let opArray:BaseOperation[] = [];
    if(opDescriptor.collections.length === opDescriptor.documents.length)
    {
        opArray.push(new RemoveMainView(opDescriptor));
        let previousValueEnc = await getPreviousValue(opDescriptor);
        tracker?.endStep("secondary_table_read");
        if(previousValueEnc === null) return new BatchOperation([], false);
        let previousValue:myTypes.ServerSideObject = myCrypto.decryptData(previousValueEnc, myCrypto.globalKey);
        Object.entries(previousValue).forEach( ([key, value]) =>
        {
            if(TStoCQLtypes.get(typeof(value)) !== undefined)
            {
                opArray.push(getRemoveSecondaryView(opDescriptor, createSecondaryInfos(previousValue, key)));
            }
        })
    }
    else
    {
        //TODO drop collection
    }
    timer.stop();
    return new BatchOperation(opArray, false);
}