import * as myTypes from "../../services/utils/myTypes";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { globalCounter } from "../../index";
import { Timer } from "../../services/monitoring/Timer";
import { getPreviousValue, SaveMainView } from "../tedOperations/MainProjections";
import { getSaveSecondaryView, TStoCQLtypes, removePreviousValue, createSecondaryInfos } from "../tedOperations/SecondaryProjections";
import { tableCreationError, BaseOperation, BatchOperation } from "../../services/database/operations/baseOperations";
import { schedulingPolicy } from "cluster";
import { fullsearchInterface } from "../../services/fullsearch/FullsearchSetup";
import { buildPath } from "../../services/utils/divers";

const noPreviousValue:Error = new Error("Unable to find a previous value");

export default async function saveRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<BatchOperation>
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
            if(opDescriptor.schema.fullsearchIndex !== undefined && opDescriptor.schema.fullsearchIndex.length > 0)
            {
                updateFullsearch(opDescriptor);
            }
        }
        
    }
    catch(err)
    {   
        console.error(err);
        if(err === noPreviousValue && opDescriptor.schema !== undefined && opDescriptor.schema.fullsearchIndex !== undefined && opDescriptor.schema.fullsearchIndex.length > 0)
        {
            indexFullsearch(opDescriptor);
        }
        if((err === noPreviousValue || err.message.substr(0,18) === "unconfigured table") || err.message.match(/^Collection ([a-zA-z_]*) does not exist./))
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

async function updateFullsearch(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    console.log("Updating index...");
    
    if(opDescriptor.clearObject === undefined) throw new Error("missing object in index operation");
    if(opDescriptor.schema === undefined || opDescriptor.schema.fullsearchIndex === undefined) throw new Error("missing schema to index object");
    let path = buildPath(opDescriptor.collections, opDescriptor.documents, false);
    await fullsearchInterface.update(opDescriptor.clearObject, opDescriptor.schema.fullsearchIndex, path);
    console.log("Done");
    
}

async function indexFullsearch(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    console.log("Indexing object...");
    if(opDescriptor.clearObject === undefined) throw new Error("missing object in index operation");
    if(opDescriptor.schema === undefined || opDescriptor.schema.fullsearchIndex === undefined) throw new Error("missing schema to index object");
    let path = buildPath(opDescriptor.collections, opDescriptor.documents, false);
    await fullsearchInterface.index(opDescriptor.clearObject, opDescriptor.schema.fullsearchIndex, path);
    console.log("Done");
    
}