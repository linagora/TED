import * as CQL from "./../BaseTools/BaseOperations";
import * as myTypes from "./../BaseTools/myTypes";
import * as secondary from "../BaseTools/SecondaryOperations";
import { key } from "./../index";
import * as myCrypto from "./../BaseTools/CryptographicTools";


export default async function saveRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<CQL.BatchOperation>
{
    if(opDescriptor.clearObject === undefined && opDescriptor.encObject === undefined) throw new Error("missing object in save operation");
    if(opDescriptor.clearObject === undefined) opDescriptor.clearObject = myCrypto.decryptData(JSON.parse(opDescriptor.encObject as string), key);
    let opArray:CQL.BaseOperation[] = [];
    try
    {
        let previousVersion:myTypes.ServerSideObject =  myCrypto.decryptData( await secondary.getPreviousValue(opDescriptor), key);
        opDescriptor.clearObject = {...previousVersion, ...opDescriptor.clearObject};
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined && previousVersion[key] !== value)
            {
                if(previousVersion[key] !== undefined ) opArray.push(secondary.removePreviousValue(opDescriptor, secondary.createSecondaryInfos(previousVersion, key)));
                opArray.push(secondary.getSaveOperationSecondaryIndex(opDescriptor, key));
            }
        });
    }
    catch
    {
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined)
            {
                opArray.push(secondary.getSaveOperationSecondaryIndex(opDescriptor, key));
            }
        });
    }
    opArray.push(new CQL.SaveOperation(opDescriptor));
    return new CQL.BatchOperation(opArray);
}