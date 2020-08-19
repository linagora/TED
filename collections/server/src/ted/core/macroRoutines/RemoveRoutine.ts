import * as myTypes from "../../services/utils/myTypes";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { globalCounter } from "../../index";
import { Timer, RequestTracker } from "../../services/monitoring/Timer";
import { RemoveMainView, getPreviousValue } from "../tedOperations/MainProjections";
import { BatchOperation, BaseOperation } from "../../services/database/operations/baseOperations";
import { TStoCQLtypes, getRemoveSecondaryView, createSecondaryInfos } from "../tedOperations/SecondaryProjections";

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
        if(opDescriptor.schema !== undefined)
        {
            for(let key of opDescriptor.schema.dbSearchIndex)
            {
                if(previousValue[key] !== undefined)
                {
                    let value = previousValue[key];
                    if(TStoCQLtypes.get(typeof(value)) !== undefined)
                    {
                        opArray.push(getRemoveSecondaryView(opDescriptor, createSecondaryInfos(previousValue, key)));
                    }
                }
            }
        }
    }
    else
    {
        //TODO drop collection
    }
    timer.stop();
    return new BatchOperation(opArray, false);
}