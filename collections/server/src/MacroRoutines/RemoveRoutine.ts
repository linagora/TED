import * as CQL from "./../BaseTools/BaseOperations";
import * as myTypes from "./../BaseTools/myTypes";
import * as secondary from "../BaseTools/SecondaryOperations";
import { key } from "./../index";
import * as myCrypto from "./../BaseTools/CryptographicTools";


export default async function removeRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<CQL.BatchOperation>
{
    let opArray:CQL.BaseOperation[] = [];
    if(opDescriptor.collections.length === opDescriptor.documents.length)
    {
        opArray.push(new CQL.RemoveOperation(opDescriptor));
        let previousValueEnc = await secondary.getPreviousValue(opDescriptor);
        if(previousValueEnc === null) return new CQL.BatchOperation([]);
        let previousValue:myTypes.ServerSideObject = myCrypto.decryptData(previousValueEnc, key);
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
    return new CQL.BatchOperation(opArray);
}