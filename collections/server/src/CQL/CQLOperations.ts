import * as myTypes from "../BaseTools/myTypes";
import * as datastaxTools from "./DatastaxTools";
import { globalCounter } from "../index";
import { Timer, RequestTracker } from "../Monitoring/Timer";

export type CQLOperation = CQLBaseOperation | CQLBatchOperation | CQLOperationArray;

export abstract class CQLBaseOperation implements myTypes.DBDirectOperation
{
  action:myTypes.action;
  table:string;
  entry:myTypes.DBentry;
  query:myTypes.Query | null;

  constructor(infos:myTypes.CQLOperationInfos)
  {
    this.action = infos.action;
    this.table = infos.table;
    this.entry = infos.keys;
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
    Object.entries(this.entry).forEach(([key,value]) =>
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
  options?:myTypes.SaveOptions;

  tableCreationFlag:boolean = false;

  constructor(infos:myTypes.CQLOperationInfos)
  {
    super(infos);
    this.options = infos.options as myTypes.SaveOptions;
    this.buildQuery();
  }

  protected buildQuery():void
  {
    let keys:string[] = ["("];
    let placeholders:string[] = ["("];
    let params:string[] = [];
    Object.entries(this.entry).forEach(([key, value]:string[]) => 
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
};

export class CQLGetOperation extends CQLBaseOperation
{
  options:myTypes.GetOptions
  pageToken?:string;

  constructor(infos:myTypes.CQLOperationInfos)
  {
    super(infos);
    this.options = infos.options as myTypes.GetOptions;
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
    let primaryKey:myTypes.Query = this.buildPrimaryKey();
    let res:string[] = ["SELECT JSON * FROM", this.table];
    let params:string[] = [];
    if(primaryKey.params.length > 0)
    {
      res.push("WHERE");
      res.push(primaryKey.query);
      params = primaryKey.params;
    }
    if(this.options.where !== undefined)
    {
      res.push("AND");
      res.push(this.options.where.field);
      res.push(this.options.where.operator);
      res.push("?");
      params.push(this.options.where.value.toString());
    }
    if(this.options.order !== undefined)
    {
      res.push("ORDER BY");
      for(let iter of this.options.order)
      {
        res.push(iter.key);
        res.push(iter.order);
        res.push(",");
      }
    }
    if(this.options.limit !== undefined)
    {
      res.push("LIMIT");
      res.push(this.options.limit.toString());
    }
    this.query = {query: res.join(" "), params: params};
  }
};

export class CQLRemoveOperation extends CQLBaseOperation
{
  constructor(infos:myTypes.CQLOperationInfos)
  {
    super(infos);
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
    let primaryKey:myTypes.Query = this.buildPrimaryKey();
    this.query = {
      query: "DELETE FROM " + this.table + " WHERE " + primaryKey.query ,
      params: primaryKey.params 
    };
  }
};

export class CQLBatchOperation implements myTypes.DBDirectOperation
{
  action:myTypes.action;
  operations:CQLBaseOperation[];
  queries:myTypes.Query[] | null;

  constructor(batch:CQLBaseOperation[])
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
    let res = await datastaxTools.runBatchDB(this.queries);
    return res;
  }

  public push(operation:CQLBaseOperation)
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
}

export class CQLOperationArray implements myTypes.DBDirectOperation
{
  action:myTypes.action;
  operations:CQLBaseOperation[];
  queries:myTypes.Query[] | null;

  constructor(batch:CQLBaseOperation[])
  {
    this.action = myTypes.action.array;
    this.operations = batch;
    for(let op of this.operations)
    {
      if(op.action === myTypes.action.batch || op.action === myTypes.action.get) throw new Error("Array cannot contain batch or get operations");
    }
    this.queries = null;
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    this.buildQueries();
    if(this.queries === null) throw new Error("Error in batch, invalid query");
    let res = await datastaxTools.runMultiOpDB(this.queries);
    return res;
  }

  public push(operation:CQLBaseOperation)
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
}