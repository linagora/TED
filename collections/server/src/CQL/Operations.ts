import * as myTypes from "../BaseTools/myTypes";
import * as datastaxTools from "./DatastaxTools";
import { TStoCQLtypes } from "../TEDOperations/SecondaryOperations";
import { globalCounter } from "../index";
import { Timer, RequestTracker } from "../Monitoring/Timer";
import { cassandra, ted } from "../Config/config";
import { createTable } from "./TableCreation";

export const tableCreationError:Error = new Error("Table creation needed, canceling operation");

export abstract class CQLBaseOperation implements myTypes.Operation
{
  action:myTypes.action;
  table:string;
  keys:myTypes.DBentry;
  query:myTypes.Query | null;

  constructor(infos:myTypes.CQLOperationInfos)
  {
    this.action = infos.action;
    this.table = infos.table;
    this.keys = infos.keys;
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
    let res:string[] = [];
    let params:string[] = [];
    Object.entries(this.keys).forEach(([key,value]) =>
    {
      res.push(key);
      res.push("= ?");
      res.push("AND");
      params.push(value);
    });
    res = res.slice(0,-1);
    return {query: res.join(" "), params: params};
  }
};

export class CQLSaveOperation extends CQLBaseOperation
{
  object:string;
  options?:myTypes.SaveOptions;

  tableCreationFlag:boolean = false;

  constructor(infos:myTypes.CQLOperationInfos)
  {
    super(infos);
    if(infos.object === undefined) throw new Error("Missing field object for a save operation");
    this.object = infos.object;
    this.options = infos.options;
    this.buildQuery();
  }

  protected buildQuery():void
  {
    if(this.object === undefined) throw new Error("Operation entry undefined");
    let entry = this.buildEntry();
    let keys:string[] = ["("];
    let placeholders:string[] = ["("];
    let params:string[] = [];
    Object.entries(entry).forEach(([key, value]:string[]) => 
    {
      keys.push(key);
      keys.push(",");
      placeholders.push("? ,");
      params.push(value)
    })
    keys = keys.slice(0,-1);
    keys.push(")");
    placeholders = placeholders.slice(0,-1);
    placeholders.push(")");
    this.query = {query: "INSERT INTO " + this.table + " " + keys.join(" ") + " VALUES " + placeholders.join(" "), params: params};

    if( this.options !== undefined)
    {
      let optionsBuilder:string[] = [" USING "];
      if(this.options.ttl != undefined){
        optionsBuilder.push("TTL");
        optionsBuilder.push(this.options.ttl.toString());
        optionsBuilder.push("AND");
      }
      //Add other options
      optionsBuilder = optionsBuilder.slice(0,-1);
      this.query.query = this.query.query + optionsBuilder.join(" ");
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
    });
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