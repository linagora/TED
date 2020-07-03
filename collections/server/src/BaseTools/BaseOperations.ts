import * as myTypes from "./myTypes";
import * as datastaxTools from "./DatastaxTools";
import { TStoCQLtypes } from "./SecondaryOperations";
import { table } from "console";
//import uuid from "uuid";


export abstract class BaseOperation implements myTypes.Operation
{
  action:myTypes.action;
  collections:string[];
  documents:string[];
  query:myTypes.Query | null;

  tableOptions:myTypes.TableOptions;
  secondaryInfos?:myTypes.WhereClause

  
  constructor(request:myTypes.InternalOperationDescription)
  {
    this.action = request.action;
    this.collections = request.collections;
    this.documents = request.documents;
    this.tableOptions = request.tableOptions;
    this.secondaryInfos = request.secondaryInfos;
    this.query = null;
  }

  protected abstract buildQuery():void;

  public async execute():Promise<myTypes.ServerAnswer>
  {
    if(this.query === null) throw new Error("unable to execute CQL operation, query not built");
    return datastaxTools.runDB(this.query);
  }

  protected buildPrimaryKey():myTypes.Query
  {
    let res = "";
    let params:string[] = [];
    for(let i:number = 0; i<this.documents.length; i++)
    {
      res = res + this.collections[i] + " = ? AND ";
      params.push(this.documents[i]);
    }
    if(this.secondaryInfos !== undefined)
    {
      res = res + this.secondaryInfos.field + " " + this.secondaryInfos.operator.toString() + " ?";
      params.push(this.secondaryInfos.value);
    }
    else res = res.slice(0,-5);
    return {query: res, params: params};
  }

  protected buildTableName():string
  {
    let res = "";
    for(let i:number = 0; i<this.collections.length; i++)
    {
      res = res + this.collections[i] + "_";
    }
    res = res.slice(0,-1);
    if(this.tableOptions.secondaryTable === true)
    {
      if(this.secondaryInfos === undefined) throw new Error("Operation is set to operate on a secondary table but no infos given");
      res = res + "__index_" + this.secondaryInfos.field;
    }
    return res
  }

  public static async createTable(tableDefinition:myTypes.TableDefinition):Promise<void>
  {
    let res = "CREATE TABLE IF NOT EXISTS " + tableDefinition.name + " (";
    let primaryKey:string = "(";
    for(let i:number = 0; i<tableDefinition.keys.length; i++)
    {
      res = res + tableDefinition.keys[i] + " " + tableDefinition.types[i] + ", ";
    }    
    for(let i:number = 0; i<tableDefinition.primaryKey.length; i++)
    {
      primaryKey = primaryKey + tableDefinition.primaryKey[i] + ", ";
    }
    primaryKey = primaryKey.slice(0,-2) + ")";
    res = res + "PRIMARY KEY " + primaryKey + ")";
    await datastaxTools.runDB({query:res, params:[]});
  
    //Add tableOtions
    //TODO
  }
};

export class SaveOperation extends BaseOperation
{
  object:string;
  options?:myTypes.SaveOptions;

  tableCreationFlag:boolean = false;

  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    console.log("New save operation =\n", request);
    if(this.documents.length != this.collections.length) throw new Error("Invalid path length parity for a save operation");
    if(request.encObject === undefined) throw new Error("Missing field object for a save operation");
    this.object = request.encObject;
    this.options = request.options;
    this.buildQuery();
  }

  protected buildQuery():void
  {
    if(this.object === undefined) throw new Error("Operation entry undefined");
    let entry = this.buildEntry();
    let tableName:string = super.buildTableName();
    let keys:string = "(";
    let placeholders:string = "(";
    let params:string[] = [];
    Object.entries(entry).forEach(([key, value]:string[]) => 
    {
      keys = keys + key + ", ";
      placeholders = placeholders + "? , ";
      params.push(value)
    })
    keys = keys.slice(0,-2) + ")";
    placeholders = placeholders.slice(0,-2) + ")";
    this.query = {query: "INSERT INTO " + tableName + " " + keys + " VALUES " + placeholders, params: params};

    if( this.options != undefined)
    {
      this.query.query = this.query.query + " USING ";
      if(this.options.ttl != undefined) this.query.query = this.query.query + " TTL " + this.options.ttl + " AND ";
      //Add other options
      this.query.query = this.query.query.slice(0,-4);
    }
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    return await super.execute()
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
      {
        this.tableCreationFlag = true;
        return await this.createTable()
        .then( () => super.execute());
      }
      return {status:"error", error:err};
    });
  }

  public async createTable():Promise<void>
  {
    let tableDefinition:myTypes.TableDefinition = {
      name: this.buildTableName(),
      keys: [],
      types: [],
      primaryKey: []
    };
    for(let i:number = 0; i<this.documents.length; i++)
    {
      tableDefinition.keys.push(this.collections[i]);
      tableDefinition.primaryKey.push(this.collections[i]);
      tableDefinition.types.push("uuid");
    }
    if(this.tableOptions.secondaryTable === true)
    {
      if(this.secondaryInfos === undefined) throw new Error("Operation is set to operate on a secondary table but no infos given");
      if(this.secondaryInfos.value === undefined) throw new Error("Missing value to know secondary index data type");
      if(this.secondaryInfos.operator !== myTypes.Operator.eq) throw new Error("Incompatible operator for a save operation");

      tableDefinition.keys = tableDefinition.keys.slice(0,-1);
      tableDefinition.types = tableDefinition.types.slice(0,-1);
      tableDefinition.primaryKey = tableDefinition.primaryKey.slice(0,-1);
      tableDefinition.keys.push(this.secondaryInfos.field);
      let columnType = TStoCQLtypes.get(typeof(this.secondaryInfos.value));
      if(columnType === undefined) throw new Error("Unsupported data type for a secondary index")
      tableDefinition.types.push(columnType);
      tableDefinition.primaryKey.push(this.secondaryInfos.field);
      tableDefinition.keys.push(this.collections.slice(-1)[0]);
      tableDefinition.primaryKey.push(this.collections.slice(-1)[0]);
      tableDefinition.types.push("uuid");
    }
    else
    {
      tableDefinition.keys.push("object");
      tableDefinition.types.push("text");
    }
    return await BaseOperation.createTable(tableDefinition);
  }

  protected buildEntry():myTypes.DBentry
  {
    let entry:myTypes.DBentry = {};
    let offset:number = this.tableOptions.secondaryTable ? 1 : 0;
    for(let i:number = 0; i<this.documents.length - offset; i++)
    {
      entry[this.collections[i]] = this.documents[i];
    }
    if(this.tableOptions.secondaryTable === true)
    {
      if(this.secondaryInfos === undefined) throw new Error("Operation is set to operate on a secondary table but no infos given");
      entry[this.secondaryInfos.field] = this.secondaryInfos.value;
      entry[this.collections.slice(-1)[0]] = this.object;
    }
    else entry["object"] = this.object;
    return entry;
  }
};

export class GetOperation extends BaseOperation
{
  filter?:myTypes.Filter;
  order?:myTypes.Order;
  limit?:number;
  pageToken?:string;

  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    this.buildQuery();
  }

  protected buildQuery():void
  {
    let tableName:string = super.buildTableName();
    let whereClause:myTypes.Query = super.buildPrimaryKey();
    this.query = {
      query: "SELECT JSON * FROM " + tableName + " WHERE " + whereClause.query ,
      params: whereClause.params
    };
    if(this.filter != undefined)
    {
      //TODO
    }
    if(this.order != undefined)
    {
      //TODO
    }
  }
};

export class RemoveOperation extends BaseOperation
{
  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    if(this.documents.length !== this.collections.length && !this.tableOptions.secondaryTable) throw new Error("Invalid path length parity for a remove operation");
    this.buildQuery();
  }

  protected buildQuery():void
  {
    let tableName:string = super.buildTableName();
    let whereClause:myTypes.Query = super.buildPrimaryKey();
    this.query = {
      query: "DELETE FROM " + tableName + " WHERE " + whereClause.query ,
      params: whereClause.params 
    };
  }
};

export class BatchOperation implements myTypes.Operation
{
  action:myTypes.action;
  operations:BaseOperation[];
  queries:myTypes.Query[] | null;

  options?:myTypes.QueryOptions;

  tableCreationFlag:boolean = false;
  tableOptions?:myTypes.TableOptions;

  constructor(batch:BaseOperation[])
  {
    this.action = myTypes.action.batch;
    this.operations = batch;
    for(let op of this.operations)
    {
      if(op.action === myTypes.action.batch || op.action === myTypes.action.get) throw new Error("Batch cannot contain batch or get operations");
    }
    this.queries = null;
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    this.buildQueries();
    if(this.queries === null) throw new Error("Error in batch, invalid query");
    return datastaxTools.runBatchDB(this.queries)
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
      {
        this.tableCreationFlag = true;
        return await this.createAllTables()
        .then( async () => 
        {
          if(this.queries === null) throw new Error("Error in batch, invalid query");
          return await datastaxTools.runBatchDB(this.queries);
        });
      }
      return {status: "error", error:err};
    });
  }

  public push(operation:BaseOperation)
  {
    if(operation.action === myTypes.action.batch || operation.action === myTypes.action.get) throw new Error("Batch cannot contain batch or get operations");
    this.operations.push(operation);
  }

  protected buildQueries():void
  {
    this.queries = [];
    for(let op of this.operations)
    {
      if(op.query === null) throw new Error("Error in batch, a base query is not built");
      this.queries.push(op.query);
    }
  }

  protected async createAllTables():Promise<void>
  {
    for(let op of this.operations)
    {
      if(op instanceof SaveOperation) await op.createTable();
    }
  }
} 


