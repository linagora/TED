import * as myTypes from "../../services/utils/myTypes";
import { createHash } from "crypto";
import { createTable } from "../../services/database/operations/baseOperations";
import { SaveOperation, GetOperation, tableCreationError, RemoveOperation } from "../../services/database/operations/baseOperations";

export let TStoCQLtypes:Map<string, string>= new Map();
TStoCQLtypes.set("string", "text");
TStoCQLtypes.set("number", "decimal");
TStoCQLtypes.set("boolean", "Boolean");

/**
 * Writes an object on a secondary table.
 * 
 * Adds or replace a document UUID on a secondary table, indexed by the value of a specified field.
 * 
 * @constructs SaveSecondaryView
 * @augments SaveOperation
 * 
 * @param {myTypes.InternalOperationDescription} request the description of the save operation
 */
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
        let res = await super.execute()
        .catch(async (err:myTypes.CQLResponseError) =>
        {
            //If the table doesn't exist, creates it and throws an error (cancel the operation, it will be retried after the table creation).
            if((err.code === 8704 && err.message.substr(0,18) === "unconfigured table") || err.message.match(/^Collection ([a-zA-z_]*) does not exist./))
            {
                await this.createTable();
                throw tableCreationError;
            }
            console.error(err);
            return {status:"error", error:err};
        });
        this.done();
        return res;
    }

    public done():void { console.log("---SecondaryView write OK"); }

    public buildTableName():string
    {
        return super.buildTableName() + "__index_" + this.secondaryInfos.secondaryKey;
    }

    /**
     * Creates a secondary table.
     * 
     * Initializes a table with these fields :
     * - [collection_names] : uuid;
     * - field_to_index : value;
     * - document_name : uuid;
     * 
     * The primary key is ( [collection_names]: uuid, field_to_index: value, document_name: uuid; ).
     * 
     * @returns {Promise<void>} Resolves when the table is created (except for Keyspace, whose tables are created asynchronously).
     */
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

/**
 * Reads an object on a secondary table.
 * 
 * Finds all the document that matches a specified field value and returns their UUIDs.
 * 
 * @constructs GetSecondaryView
 * @augments GetOperation
 * 
 * @param {myTypes.InternalOperationDescription} request the description of the operation
 */
export class GetSecondaryView extends GetOperation
{
    secondaryInfos:myTypes.SecondaryInfos;

    constructor(request:myTypes.InternalOperationDescription)
    {
        super(request);
        if(request.secondaryInfos === undefined) throw new Error("Missing secondary arguments to create secondary operation");
        this.secondaryInfos = request.secondaryInfos;
        if(this.options === undefined) this.options = {};
        if(request.secondaryInfos.secondaryValue !== null)
            this.options.where = this.getWhere(request.secondaryInfos);        
        this.table = this.buildTableName();        
        this.buildOperation();        
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        let res = await super.execute();
        this.done();
        return res;
    }

    public done():void { console.log("---SecondaryView read OK"); }

    public buildTableName():string
    {
        return super.buildTableName() + "__index_" + this.secondaryInfos.secondaryKey;
    }

    protected buildEntry():myTypes.DBentry
    {
        let entry:myTypes.DBentry = {};
        for(let i:number = 0; i<this.collections.length -1 ; i++)
        {
            entry[this.collections[i]] = this.documents[i];
        }
        return entry;
    }

    protected getWhere(secondaryInfos:myTypes.SecondaryInfos):myTypes.WhereClause
    {
        return {
            key: secondaryInfos.secondaryKey,
            value: secondaryInfos.secondaryValue,
            operator: secondaryInfos.operator
        };
    }

    public async createTable():Promise<void>{}
};

/**
 * Remove an object from a secondary table.
 * 
 * Removes an object from a a secondary table with its value and its UUID.
 * 
 * @constructs RemoveSecondaryView
 * @augments RemoveOperation
 * 
 * @param {myTypes.InternalOperationDescription} request the description of the operation
 */
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
        this.done();
        return res;
    }

    public done():void { console.log("---SecondaryView remove OK"); }

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

/**
 * Computes a Save operation on a secondary table, from another operation and the name of the field.
 * @param {myTypes.InternalOperationDescription} operation the operation that needs to store a value on a secondary table.
 * @param {string} secondaryKey the field of the object that needs to be stored.
 * @returns {SaveSecondaryView} the save operation on the secondary table.
 */
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
 /**
  * Computes a Get operation on a secondary table, from another operation and the name of the field.
  * @param {myTypes.InternalOperationDescription} operation the operation that needs to read a value on a secondary table.
  * @param {myTypes.SecondaryInfos} where an optionnal filter to apply on the query. If not provided, ` operation ` must contain a secondaryInfos field.
  * @returns {GetSecondaryView} the get operation on the secondary table.
  */
export function getGetSecondaryView(operation:myTypes.InternalOperationDescription, where?:myTypes.SecondaryInfos):GetSecondaryView
{
    let options:myTypes.GetOptions = {};
    if(operation.options !== undefined) options = operation.options as myTypes.GetOptions;
    if(operation.secondaryInfos === undefined && where === undefined) throw new Error("Unable to find an object in a secondary table without a condition");
    let op = new GetSecondaryView({
        action: myTypes.action.get,
        opID: operation.opID,
        collections: operation.collections,
        documents: operation.documents,
        secondaryInfos: where === undefined ? hashSecondaryInfos(operation.secondaryInfos as myTypes.SecondaryInfos) : hashSecondaryInfos(where),
        options : options,
    });
    return op;
}

 /**
  * Computes a Remove operation on a secondary table, from another operation and the name of the field.
  * @param {myTypes.InternalOperationDescription} operation the operation that needs to remove a value from a secondary table.
  * @param {myTypes.SecondaryInfos} where an optionnal filter to apply on the query. If not provided, ` operation ` must contain a secondaryInfos field.
  * @returns {RemoveSecondaryView} the remove operation on the secondary table.
  */
export function getRemoveSecondaryView(operation:myTypes.InternalOperationDescription, where?:myTypes.SecondaryInfos):RemoveSecondaryView
{
    if(operation.secondaryInfos === undefined && where === undefined) throw new Error("Unable to find an object in a secondary table without a condition");
    let op= new RemoveSecondaryView({
        action: myTypes.action.remove,
        opID: operation.opID,
        collections : operation.collections,
        documents: operation.documents,
        secondaryInfos: where === undefined ? hashSecondaryInfos(operation.secondaryInfos as myTypes.SecondaryInfos) : hashSecondaryInfos(where),
    });
    return op;
}


/**
 * Compute a filter clause from an object, a field name and an operator.
 * @param {myTypes.ServerSideObject} object the reference object, the value in the clause will be ` object[secondaryKey] `.
 * @param {string} secondaryKey the field to use.
 * @param {myTypes.Operator} operator the operator to use in the clause.
 * @returns {myTypes.SecondaryInfos} the clause formatted correctly.
 */
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

/**
 * Hashes the value of a where clause (used for string only).
 * @param {myTypes.WhereClause | myTypes.SecondaryInfos} object the clause whose value must be hashe.
 * @returns {myTypes.SecondaryInfos} another clause with a hashed value.
 */
export function hashSecondaryInfos(object:myTypes.WhereClause | myTypes.SecondaryInfos):myTypes.SecondaryInfos
{

    if(Object.keys(object).includes("key"))
    {
        let where = object as myTypes.WhereClause;
        if(typeof(where.value) === "string")
        {
            return {
                secondaryKey:where.key,
                secondaryValue: createHash("sha256").update(where.value as string).digest('base64'),
                operator: where.operator
            }
        }
        return {
            secondaryKey: where.key,
            secondaryValue: where.value,
            operator: where.operator
        }
    }
    else
    {
        let secondary = object as myTypes.SecondaryInfos;
        if(typeof(secondary.secondaryValue) === "string")
        {
            return {
                secondaryKey : secondary.secondaryKey,
                secondaryValue : createHash("sha256").update(secondary.secondaryValue as string).digest('base64'),
                operator : secondary.operator
            }
        }
        return secondary;
    }
}

export function removePreviousValue(opDescriptor:myTypes.InternalOperationDescription, where:myTypes.SecondaryInfos):RemoveSecondaryView
{
    if( opDescriptor.collections.length !== opDescriptor.documents.length) throw new Error("Need the object ID to remove its previuous value");
    return getRemoveSecondaryView(opDescriptor, where);
}

