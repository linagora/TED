import * as myTypes from "../../services/utils/myTypes";
import { forwardCollection } from "./StoredTaskHandling";
import { globalCounter } from "../../index";
import { Timer } from "../../services/monitoring/Timer";
import { GetMainView } from "../tedOperations/MainProjections";
import { getGetSecondaryView } from "../tedOperations/SecondaryProjections";
import { fullsearchInterface } from "../../services/fullsearch/FullsearchSetup";
import { buildPath } from "../../services/utils/divers";


export let EmptyResultError = new Error("No matching object found");

export default async function getRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<GetMainView>
{
    /**
     * Computes a MainView read operation based on a read request.
     * 
     * Handles every possible case for a read operation (except the fullsearch which has been catched higher), runs the eventual secondary operations (collection forwarding, secondary reading), and returns a Get operation on the MainView which will return the desired result(s).
     * 
     * @param {myTypes.InternalOperationDescription} var The description of the request that needs to be handled.
     * 
     * @returns {Promise<GetMainView>} A MainView read operation, returning the results asked by the request.
     */

    globalCounter.inc("get_precompute");
    let timer = new Timer("get_precompute");

    //Before reading on a collection, make sure all the operations on this collection have been computed
    await forwardCollection(opDescriptor);

    //Case 1: the path specifies the document UUID 
        //=> read the document directly on MainView
    if(opDescriptor.collections.length ===  opDescriptor.documents.length) return new GetMainView(opDescriptor);

    try
    {
        //Case 2 : the path specifies the collection, and no filter has been given 
            //=> read all the collection
        if(opDescriptor.secondaryInfos === undefined) return new GetMainView(opDescriptor);

        //Case 3 : the query is a "Where" query on the collection 
            //=> get the UUIDs of the documents matching the query and then read them on MainView
        let matchingIDs:string[] = await getMatchingIDs(opDescriptor);
        timer.stop();
        if(matchingIDs.length === 0) throw EmptyResultError;
        let op = buildGetOperation(opDescriptor, matchingIDs);
        return op;
    }
    catch(err)
    {
        //If there was no matching result to the query on the secondary tables, there is no need to return an operation and the error is used to jump directly to the answer.
        if(err === EmptyResultError) throw err;
        console.error(err);
        timer.stop();
        return new GetMainView(opDescriptor);
    }
}

async function getMatchingIDs(opDescriptor:myTypes.InternalOperationDescription):Promise<string[]>
{
    /**
     * Compute a list of all the documents of a collection matching a "Where" clause.
     * 
     * Compute a string array containing the UUIDs of all the documents of a collection matching a "Where" clause on a single indexed field.
     * 
     * @param {myTypes.InternalOperationDescription} var Description of the request that requires a search on the secondary tables.
     * 
     * @returns {Promise<string[]>} An array containing the UUIDs of all the matching documents.
     */

    //Querying all the matching result on the secondary table
    let secondaryOp = getGetSecondaryView(opDescriptor);
    let result:myTypes.ServerAnswer = await secondaryOp.execute();

    //Formating the result with only the desired UUIDs
    let objectKey:string = opDescriptor.collections.slice(-1)[0];
    let matchingIDs:string[] = []

    if(result.queryResults === undefined || result.queryResults.allResultsClear === undefined) throw new Error("Unable to query requested fields : " + JSON.stringify(result));
    for(let object of result.queryResults.allResultsClear)
    {
        matchingIDs.push(object[objectKey] as string);
    }
    return matchingIDs;        
}

function buildGetOperation(opDescriptor:myTypes.InternalOperationDescription, matchingIDs: string[]):GetMainView
{
    /**
     * Builds a MainView read operation from an UUIDs array.
     * 
     * Uses the matching UUIDs from the previous function, and a read request description to build a GetMainView operation.
     * 
     * @param {myTypes.InternalOperationDescription} var The description of the original request.
     * @param {string[]} var A list of all the documents to retrieve from the collection.
     * 
     * @returns {GetMainView} A MainView read operation which returns all the documents from the array.
     */

    //Copying the options into the returned operation.
    let options:myTypes.GetOptions = {};
    if(opDescriptor.options !== undefined) options = opDescriptor.options as myTypes.GetOptions;

    let op = new GetMainView({
        action: myTypes.action.get,
        opID: opDescriptor.opID,
        documents: opDescriptor.documents,
        collections: opDescriptor.collections,
        options: {
            where:{
                key: opDescriptor.collections.slice(-1)[0],
                value: matchingIDs,
                operator: myTypes.Operator.in
            },
            limit: options.limit,
            fullsearch: options.fullsearch,
            order: options.order,
            pageToken: options.pageToken,
        }
    });
    return op
}

export async function fullsearchRequest(opDescriptor:myTypes.InternalOperationDescription):Promise<GetMainView[]>
{   
    /**
     * Runs a fullsearch request.
     * 
     * Runs a query on the fullsearch core in order to get the UUIDs matching the query. Then computes MainView read operations from the returned results.
     * 
     * @param {myTypes.InternalOperationDescription} var The original request requiring a fullsearch query.
     * 
     * @returns {Promise<GetMainView[]>} A MainView read operation for each document returned by the fullsearch (potentially on different collections). 
     */

    let path = buildPath(opDescriptor.collections, opDescriptor.documents,false);
    let matchingKeys:myTypes.ServerSideObject[] = await fullsearchInterface.search((opDescriptor.options as myTypes.GetOptions).fullsearch, path);
    return buildFullsearchGet(opDescriptor, matchingKeys);
}

function buildFullsearchGet(opDescriptor:myTypes.InternalOperationDescription, matchingKeys: myTypes.ServerSideObject[]):GetMainView[]
{
    /**
     * Computes GetMainView operations from a fullsearch resutls.
     * 
     * @param {myTypes.InternalOperationDescription} var The original request requiring a fullsearch query.
     * @param {myTypes.ServerSideObject[]} var An array of object containing the primaryKey of the documents to query.
     * 
     * @returns {GetMainView[]} A list of MainView read operations, returning each a document matching the fullsearch query.
     */

    let res:GetMainView[] = [];
    for(let keys of matchingKeys)
    {
        let documents:string[] = [];
        for(let coll of opDescriptor.collections)
        {
            documents.push(keys[coll] as string);
        }
        res.push(new GetMainView({
            action: myTypes.action.get,
            opID: opDescriptor.opID,
            documents: documents,
            collections: opDescriptor.collections,
        }));
    }
    return res;
}