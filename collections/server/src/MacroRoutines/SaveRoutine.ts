import * as CQL from "./../BaseTools/BaseOperations";
import * as myTypes from "./../BaseTools/myTypes";
import * as secondary from "../BaseTools/SecondaryOperations";
import * as myCrypto from "./../BaseTools/CryptographicTools";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";

export default async function saveRequest(opDescriptor:myTypes.InternalOperationDescription, tracker?:RequestTracker):Promise<CQL.BatchOperation>
{
    if(opDescriptor.clearObject === undefined && opDescriptor.encObject === undefined) throw new Error("missing object in save operation");
    if(opDescriptor.clearObject === undefined) opDescriptor.clearObject = myCrypto.decryptData(JSON.parse(opDescriptor.encObject as string), myCrypto.globalKey);
    globalCounter.inc("save precompute");
    let timer = new Timer("save precompute");
    let opArray:CQL.BaseOperation[] = [];
    try
    {
        console.log("getting previous value");
        let previousValueEnc = await secondary.getPreviousValue(opDescriptor);
        tracker?.endStep("secondary table read");
        if(previousValueEnc === null) throw new Error("Unable to find a previous value");
        let previousVersion:myTypes.ServerSideObject =  myCrypto.decryptData( previousValueEnc, myCrypto.globalKey);
        console.log("previous value = ", JSON.stringify(previousVersion));
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
    catch(err)
    {   
        console.error(err);
        Object.entries(opDescriptor.clearObject).forEach( ([key, value]) =>
        {
            if(secondary.TStoCQLtypes.get(typeof(value)) !== undefined)
            {
                opArray.push(secondary.getSaveOperationSecondaryIndex(opDescriptor, key));
            }
        });
    }
    myCrypto.encryptOperation(opDescriptor, myCrypto.globalKey);
    opArray.push(new CQL.SaveOperation(opDescriptor));
    timer.stop();
    return new CQL.BatchOperation(opArray);
}