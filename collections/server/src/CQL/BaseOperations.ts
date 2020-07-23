import * as myTypes from "../BaseTools/myTypes";
import * as datastaxTools from "./DatastaxTools";
import { TStoCQLtypes } from "../TEDOperations/SecondaryOperations";
import { globalCounter } from "../index";
import { Timer, RequestTracker } from "../Monitoring/Timer";
import { cassandra, ted } from "../Config/config";
import { createTable } from "./TableCreation";

export const tableCreationError:Error = new Error("Table creation needed, canceling operation");

export abstract class BaseOperation implements myTypes.Operation
{
  action:myTypes.action;
  collections:string[];
  documents:string[];
  opID:string;
  query:myTypes.Query | null;

  tableOptions:myTypes.TableOptions;
  secondaryInfos?:myTypes.WhereClause;
  canCreateTable:boolean;

  
  constructor(request:myTypes.InternalOperationDescription)
  {
    this.action = request.action;
    this.collections = request.collections;
    this.documents = request.documents;
    this.tableOptions = request.tableOptions;
    this.secondaryInfos = request.secondaryInfos;
    this.canCreateTable = false;
    this.opID = request.opID;
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

  public buildTableName():string
  {
    if(this.tableOptions.tableName !== undefined) return this.tableOptions.tableName;
    let res = cassandra.keyspace + ".";
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

  public abstract async createTable():Promise<void>;
};

export class SaveOperation extends BaseOperation
{
  object:string;
  options?:myTypes.SaveOptions;

  tableCreationFlag:boolean = false;

  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    if(this.documents.length != this.collections.length) throw new Error("Invalid path length parity for a save operation");
    if(request.encObject === undefined) throw new Error("Missing field object for a save operation");
    this.object = request.encObject;
    this.options = request.options;
    this.canCreateTable = true;
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
    globalCounter.inc("cql_save");
    let timer = new Timer("cql_save");
    return await super.execute()
    .then( (result) => 
    {
      timer.stop();
      return result;
    })
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      timer.stop();
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
    return await createTable(tableDefinition);
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
  order?:string;
  limit?:number;
  pageToken?:string;

  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    if(request.options !== undefined)
    {
      this.order = request.options.order;
      this.limit = request.options.limit;
    }
    this.buildQuery();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    globalCounter.inc("cql_get");
    let timer = new Timer("cql_get");
    let res = await super.execute();
    timer.stop();
    return res;
  }

  protected buildQuery():void
  {
    let tableName:string = this.buildTableName();
    let whereClause:myTypes.Query = this.buildPrimaryKey();
    this.query = {
      query: "SELECT JSON * FROM " + tableName ,
      params: []
    };
    if(whereClause.params.length > 0)
    {
      this.query.query = this.query.query + " WHERE " + whereClause.query;
      this.query.params = whereClause.params;
    }
    if(this.order !== undefined)
    {
      this.query.query = this.query.query + " ORDER BY " + this.order;
    }
    if(this.limit !== undefined)
    {
      this.query.query = this.query.query + " LIMIT " + this.limit;
    }
  }

  public async createTable():Promise<void>{}
};

export class RemoveOperation extends BaseOperation
{
  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    if(this.documents.length !== this.collections.length && !this.tableOptions.secondaryTable) throw new Error("Invalid path length parity for a remove operation");
    this.buildQuery();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    globalCounter.inc("cql_remove");
    let timer = new Timer("cql_remove");
    let res = await super.execute();
    timer.stop();
    return res;
  }

  protected buildQuery():void
  {
    let tableName:string = this.buildTableName();
    let whereClause:myTypes.Query = this.buildPrimaryKey();
    this.query = {
      query: "DELETE FROM " + tableName + " WHERE " + whereClause.query ,
      params: whereClause.params 
    };
  }

  public async createTable():Promise<void>{}
};

export class BatchOperation implements myTypes.Operation
{
  action:myTypes.action;
  operations:BaseOperation[];
  queries:myTypes.Query[] | null;

  tracker?:RequestTracker;

  constructor(batch:BaseOperation[], tracker?:RequestTracker)
  {
    this.action = myTypes.action.batch;
    this.operations = batch;
    this.tracker = tracker;
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
    let res = await datastaxTools.runBatchDB(this.queries, undefined, this.tracker)
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      this.tracker?.endStep("first_attempt");
      if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
      {
        if(ted.enableMultiTableCreation) this.createAllTables();
        else this.createTable(err.message);
        console.log("top");
        throw tableCreationError;
      }
      return {status: "error", error: err};
    });
    return res;
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
    let promises:Promise<void>[] = [];
    for(let op of this.operations)
    {
      if(op.canCreateTable) promises.push(op.createTable());
    }
    await Promise.all(promises);
  }

  protected async createTable(errmsg:string):Promise<void>
  {
    let parse = errmsg.match(/[\.a-zA-Z0-9_]*$/);
    if(parse === null) throw new Error("Unable to parse table name in batch error");
    let tableName = parse[0];
    for(let op of this.operations)
    {
      let tmp = op.buildTableName();
      if(tmp === tableName)
      {
        await op.createTable();
        return;
      }
    }
    throw new Error("Unable to find which operation triggered the error inside the batch " + tableName);
  }
}