import * as myTypes from "../../services/utils/myTypes";
import { createHash } from "crypto";
import { createTable } from "../../services/database/adapters/cql/TableCreation";
import { SaveOperation, GetOperation, tableCreationError, RemoveOperation } from "../../services/database/operations/baseOperations";

export let TStoCQLtypes:Map<string, string>= new Map();
TStoCQLtypes.set("string", "text");
TStoCQLtypes.set("number", "decimal");
TStoCQLtypes.set("boolean", "Boolean");


export class SaveSecondaryView extends SaveOperation
{
    secondaryInfos:myTypes.SecondaryInfos;

    constructor(request:myTypes.InternalOperationDescription)
    {
        super(request);
        if(request.secondaryInfos === undefined) throw new Error("Missing secondary arguments to create secondary operation");
        this.secondaryInfos = request.secondaryInfos;
        this.canCreateTable = true;
        this.table = this.buildTableName();
        this.buildOperation();
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        return super.execute()
        .catch(async (err:myTypes.CQLResponseError) =>
        {
            if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
            {
                await this.createTable();
                throw tableCreationError;
            }
            return {status:"error", error:err};
        });
    }

    public buildTableName():string
    {
        return super.buildTableName() + "__index_" + this.secondaryInfos.secondaryKey;
    }

    public async createTable():Promise<void>
    {
        let tableDefinition:myTypes.TableDefinition = {
            name: this.buildTableName(),
            keys: [],
            types: [],
            primaryKey: []
        };
        if(this.secondaryInfos.operator !== myTypes.Operator.eq) throw new Error("Incompatible operator for a save operation");

        for(let i:number = 0; i<this.documents.length - 1; i++)
        {
          tableDefinition.keys.push(this.collections[i]);
          tableDefinition.primaryKey.push(this.collections[i]);
          tableDefinition.types.push("uuid");
        }
        let columnType = TStoCQLtypes.get(typeof(this.secondaryInfos.secondaryValue));
        if(columnType === undefined) throw new Error("Unsupported data type for a secondary index");

        tableDefinition.keys.push(this.secondaryInfos.secondaryKey);
        tableDefinition.types.push(columnType);
        tableDefinition.primaryKey.push(this.secondaryInfos.secondaryKey);
        tableDefinition.keys.push(this.collections.slice(-1)[0]);
        tableDefinition.types.push("uuid");
        tableDefinition.primaryKey.push(this.collections.slice(-1)[0]);
        
        return createTable(tableDefinition);
    }

    protected buildEntry():myTypes.DBentry
    {
        let entry:myTypes.DBentry = {};
        for(let i:number = 0; i<this.documents.length - 1; i++)
        {
            entry[this.collections[i]] = this.documents[i];
        }
        entry[this.secondaryInfos.secondaryKey] = this.secondaryInfos.secondaryValue;
        entry[this.collections.slice(-1)[0]] = this.documents.slice(-1)[0];
        return entry;
    }
};

export class GetSecondaryView extends GetOperation
{
    secondaryInfos:myTypes.SecondaryInfos;

    constructor(request:myTypes.InternalOperationDescription)
    {
        super(request);
        if(request.secondaryInfos === undefined) throw new Error("Missing secondary arguments to create secondary operation");
        this.secondaryInfos = request.secondaryInfos;
        this.table = this.buildTableName();
        this.buildOperation();
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        let res = await super.execute();
        return res;
    }

    public buildTableName():string
    {
        return super.buildTableName() + "__index_" + this.secondaryInfos.secondaryKey;
    }

    protected buildEntry():myTypes.DBentry
    {
        let entry:myTypes.DBentry = {};
        for(let i:number = 0; i<this.documents.length -1 ; i++)
        {
            entry[this.collections[i]] = this.documents[i];
        }
        entry[this.secondaryInfos.secondaryKey] = this.secondaryInfos.secondaryValue;
        return entry;
    }

    public async createTable():Promise<void>{}
};

export class RemoveSecondaryView extends RemoveOperation
{
    secondaryInfos:myTypes.SecondaryInfos;

    constructor(request:myTypes.InternalOperationDescription)
    {
        super(request);
        if(request.secondaryInfos === undefined) throw new Error("Missing secondary arguments to create secondary operation");
        this.secondaryInfos = request.secondaryInfos;
        this.table = this.buildTableName();
        this.buildOperation();
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        let res = await super.execute();
        return res;
    }

    public buildTableName():string
    {
        return super.buildTableName() + "__index_" + this.secondaryInfos.secondaryKey;
    }

    protected buildEntry():myTypes.DBentry
    {
        let entry:myTypes.DBentry = {};
        for(let i:number = 0; i<this.documents.length ; i++)
        {
            entry[this.collections[i]] = this.documents[i];
        }
        entry[this.secondaryInfos.secondaryKey] = this.secondaryInfos.secondaryValue;
        entry[this.collections.slice(-1)[0]] = this.documents.slice(-1)[0];
        return entry;
    }

    public async createTable():Promise<void>{}
};


export function getSaveSecondaryView(operation:myTypes.InternalOperationDescription, secondaryKey:string):SaveSecondaryView
{
    if(operation.clearObject === undefined) throw new Error("Missing object for a secondary index update");
    let op = new SaveSecondaryView({
        action: myTypes.action.save,
        opID: operation.opID,
        collections: operation.collections,
        documents: operation.documents,
        encObject: operation.encObject,
        secondaryInfos: createSecondaryInfos(operation.clearObject, secondaryKey),
    });
    return op;
}

export function getGetSecondaryView(operation:myTypes.InternalOperationDescription, where?:myTypes.SecondaryInfos):GetSecondaryView
{
    if(operation.secondaryInfos === undefined && where === undefined) throw new Error("Unable to find an object in a secondary table without a condition");
    let op = new GetSecondaryView({
        action: myTypes.action.get,
        opID: operation.opID,
        collections: operation.collections,
        documents: operation.documents,
        secondaryInfos: operation.secondaryInfos !== undefined ? operation.secondaryInfos : where,
    });
    return op;
}

export function getRemoveSecondaryView(operation:myTypes.InternalOperationDescription, where?:myTypes.SecondaryInfos):RemoveSecondaryView
{
    if(operation.secondaryInfos === undefined && where === undefined) throw new Error("Unable to find an object in a secondary table without a condition");
    let op= new RemoveSecondaryView({
        action: myTypes.action.remove,
        opID: operation.opID,
        collections : operation.collections,
        documents: operation.documents,
        secondaryInfos: where === undefined ? operation.secondaryInfos : where,
    });
    return op;
}



export function createSecondaryInfos(object:myTypes.ServerSideObject, secondaryKey:string, operator:myTypes.Operator = myTypes.Operator.eq):myTypes.SecondaryInfos
{
    if(object[secondaryKey] === undefined) throw new Error("Secondary key doesn't exist in object");
    if(typeof(object[secondaryKey]) === "string")
    {
        return {
            secondaryKey:secondaryKey,
            secondaryValue: createHash("sha256").update(object[secondaryKey] as string).digest('base64'),
            operator: operator
        }
    }
    return {
        secondaryKey: secondaryKey,
        secondaryValue: object[secondaryKey],
        operator: operator
    }
}

export function removePreviousValue(opDescriptor:myTypes.InternalOperationDescription, where:myTypes.SecondaryInfos):RemoveSecondaryView
{
    if( opDescriptor.collections.length !== opDescriptor.documents.length) throw new Error("Need the object ID to remove its previuous value");
    return getRemoveSecondaryView(opDescriptor, where);
}

