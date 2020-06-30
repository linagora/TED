import * as CQL from "./BaseOperations";
import * as myTypes from "./myTypes";

export function processPath(path:string):{documents:string[], collections:string[]}
{
    if(path === undefined) return {documents:[], collections:[]};
    path = path.toLowerCase();
    let documents:string[] = [];
    let collections:string[] = [];
    let names:string[] = path.split('/');
    for(let i:number = 0; i<names.length; i++)
    {
        if(i%2 === 0)
        {
            collections.push(names[i]);
            let nameCtrl = names[i].match("^[a-z]*$");
            if(nameCtrl === null) throw new Error("Invalid collection name");
        } 
        else
        {
            documents.push(names[i]);
            let nameCtrl = names[i].match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
            if(nameCtrl === null) throw new Error("Invalid document ID");
        }      
    }
    return {documents:documents, collections:collections};
}

export function createOperation(opDescriptor:myTypes.InternalOperationDescription):myTypes.Operation
{
    try
    {
        if((opDescriptor.collections === [] || opDescriptor.documents === []) && opDescriptor.action !== myTypes.action.batch) throw new Error("missing field path in request");
        let op:CQL.BaseOperation;
        switch(opDescriptor.action)
        {
            case myTypes.action.batch:
            {
                if(opDescriptor.operations === undefined) throw new Error("missing field operations in batch request");
                let batch = new CQL.BatchOperation([]);
                for(let req of opDescriptor.operations)
                {
                    let operation = createOperation(req);
                    if(!(operation instanceof CQL.BaseOperation)) throw new Error("Only base operations are allowed in a batch");
                    batch.push(operation);
                }
                return batch;
            }
            case myTypes.action.save:
            {
                if(opDescriptor.encObject == undefined) throw new Error("Encrypted object not created in save request");
                op = new CQL.SaveOperation(opDescriptor);
                return op;
            }
            case myTypes.action.get:
            {
                op = new CQL.GetOperation(opDescriptor);
                return op;
            }
            case myTypes.action.remove:
            {
                let op = new CQL.RemoveOperation(opDescriptor);
                return op;
            }
            default:
            {
                throw new Error("Unknown action in request");
            }
        }
    }
    catch(err)
    {
        console.log(opDescriptor);
        console.log("Failed to create operation: \n",err);
        throw err;
    }
};

export function getInternalOperationDescription(request:myTypes.ServerBaseRequest):myTypes.InternalOperationDescription
{
    let processedPath = processPath(request.path);
    let opDescriptor:myTypes.InternalOperationDescription = {
        action: request.action,
        documents: processedPath.documents,
        collections: processedPath.collections,
        clearObject: request.object,
        options: request.options
    }
    if(request.operations !== undefined)
    {
        opDescriptor.operations = [];
        for(let op of request.operations)
        {
            opDescriptor.operations.push(getInternalOperationDescription(op));
        }
    }
    return opDescriptor;
}