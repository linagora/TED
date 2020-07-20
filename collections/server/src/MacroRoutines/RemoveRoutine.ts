import * as CQL from "./../BaseTools/BaseOperations";
import * as myTypes from "./../BaseTools/myTypes";
import * as secondary from "../BaseTools/SecondaryOperations";
import * as myCrypto from "./../BaseTools/CryptographicTools";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";

export default async function removeRequest(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<CQL.BatchOperation>
{
    globalCounter.inc("remove_precompute");
    let timer = new Timer("remove_precompute");
    let opArray:CQL.BaseOperation[] = [];
    if(opDescriptor.collections.length === opDescriptor.documents.length)
    {
        opArray.push(new CQL.RemoveOperation(opDescriptor));
        let previousValueEnc = await secondary.getPreviousValue(opDescriptor);
        tracker?.endStep("secondary_table_read");
        if(previousValueEnc === null) return new CQL.BatchOperation([]);
        let previousValue:myTypes.ServerSideObject = myCrypto.decryptData(previousValueEnc, myCrypto.globalKey);
        Object.entries(previousValue).forEach( ([key, value]) =>
        {
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined)
            {
                opArray.push(secondary.getRemoveOperationSecondaryIndex(opDescriptor, secondary.createSecondaryInfos(previousValue, key)));
            }
        })
    }
    else
    {
        //TODO drop collection
    }
    timer.stop();
    return new CQL.BatchOperation(opArray);
}