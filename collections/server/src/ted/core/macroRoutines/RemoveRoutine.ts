import * as myTypes from "../../services/utils/myTypes";
import * as myCrypto from "../../services/utils/cryptographicTools";
import { globalCounter } from "../../index";
import { Timer } from "../../services/monitoring/Timer";
import { RemoveMainView, getPreviousValue } from "../tedOperations/MainProjections";
import { BatchOperation, BaseOperation } from "../../services/database/operations/baseOperations";
import { TStoCQLtypes, getRemoveSecondaryView, createSecondaryInfos } from "../tedOperations/SecondaryProjections";
import { fullsearchInterface } from "../../services/fullsearch/FullsearchSetup";
import { buildPath } from "../../services/utils/divers";

/**
 * Computes a MainView remove operation based on a remove request.
 * 
 * Handles every possible case for a remove operation, computes the eventual secondary operations, and returns a Batch operation which will apply all the desired mofications on TED.
 * 
 * @param {myTypes.InternalOperationDescription} opDescriptor The description of the request that needs to be handled.
 * 
 * @returns {Promise<BatchOperation>} A Batch with remove operations on both MainView and secondary tables.
 */
export default async function removeRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<BatchOperation>
{
    globalCounter.inc("remove_precompute");
    let timer = new Timer("remove_precompute");

    let opArray:BaseOperation[] = [];

    //Case 1 : the request requires a document deletion.
    if(opDescriptor.collections.length === opDescriptor.documents.length)
    {
        //Removes the document on the MainView
        opArray.push(new RemoveMainView(opDescriptor));

        //Computes the remove operations on all the secondary tables.
        let previousValueEnc = await getPreviousValue(opDescriptor);
        if(previousValueEnc === null) return new BatchOperation([], false); //If the object doesn't exist nothing needs to be done.

        
        let previousValue:myTypes.ServerSideObject = myCrypto.decryptData(previousValueEnc, myCrypto.globalKey);
        if(opDescriptor.schema !== undefined)
        {
            //For each key of the specified scheme, if the key exists in the object then it needs to be deleted.
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
            //If the document is indexed on the fullsearch core it needs to be removed there too.
            if(opDescriptor.schema.fullsearchIndex !== undefined && opDescriptor.schema.fullsearchIndex.length > 0)
            {
                deleteFullsearch(opDescriptor);
            }
        }
    }
    //Case 2 : the request requires a collection deletion.
    else
    {
        //TODO drop collection
    }
    timer.stop();
    return new BatchOperation(opArray, false);
}

/**
 * Deletes a document from the fullsearch index.
 * 
 * Runs a remove operation on the fullsearch core, based on the document path and on the scheme of the collection.
 * 
 * @param {myTypes.InternalOperationDescription} opDescriptor The original remove request.
 * 
 * @returns {Promise<void>} Resolves when the documents is removed.
 */
async function deleteFullsearch(opDescriptor:myTypes.InternalOperationDescription):Promise<void>
{
    if(opDescriptor.schema === undefined || opDescriptor.schema.fullsearchIndex === undefined) throw new Error("missing schema to index object");
    let path = buildPath(opDescriptor.collections, opDescriptor.documents, false);
    return fullsearchInterface.delete(opDescriptor.schema.fullsearchIndex, path);
}