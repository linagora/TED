import * as myTypes from "../../services/utils/myTypes";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { globalCounter } from "../../index";
import { Timer } from "../../services/monitoring/Timer";
import { RemoveMainView, getPreviousValue } from "../tedOperations/MainProjections";
import { BatchOperation, BaseOperation } from "../../services/database/operations/baseOperations";
import { TStoCQLtypes, getRemoveSecondaryView, createSecondaryInfos } from "../tedOperations/SecondaryProjections";

export default async function removeRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<BatchOperation>
{
    globalCounter.inc("remove_precompute");
    let timer = new Timer("remove_precompute");
    let opArray:BaseOperation[] = [];
    if(opDescriptor.collections.length === opDescriptor.documents.length)
    {
        opArray.push(new RemoveMainView(opDescriptor));
        let previousValueEnc = await getPreviousValue(opDescriptor);
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
                        opArray.push(getRemoveSecondaryView(opDescriptor, {secondaryValue: previousValue[key], secondaryKey: key, operator: myTypes.Operator.eq}));
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