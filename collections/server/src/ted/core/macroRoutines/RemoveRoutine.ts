import * as myTypes from "../../services/utils/myTypes";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { globalCounter } from "../../index";
import { Timer } from "../../services/monitoring/Timer";
import { RemoveMainView, getPreviousValue } from "../tedOperations/MainProjections";
import { BatchOperation, BaseOperation } from "../../services/database/operations/baseOperations";
import { TStoCQLtypes, getRemoveSecondaryView, createSecondaryInfos } from "../tedOperations/SecondaryProjections";
import { fullsearchInterface } from "../../services/fullsearch/FullsearchSetup";
import { buildPath } from "../../services/utils/divers";

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
            if(opDescriptor.schema.fullsearchIndex !== undefined && opDescriptor.schema.fullsearchIndex.length > 0)
            {
                deleteFullsearch(opDescriptor);
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

async function deleteFullsearch(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    if(opDescriptor.schema === undefined || opDescriptor.schema.fullsearchIndex === undefined) throw new Error("missing schema to index object");
    let path = buildPath(opDescriptor.collections, opDescriptor.documents, false);
    fullsearchInterface.delete(opDescriptor.schema.fullsearchIndex, path);
}