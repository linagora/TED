import * as myTypes from "./myTypes";
import * as datastaxTools from "./DatastaxTools";
import { table } from "console";
import { kMaxLength } from "buffer";
import { Serializer } from "v8";
//import uuid from "uuid";


abstract class BaseOperation implements myTypes.Operation
{
  action:myTypes.action;
  path:string;
  collections:string[];
  documents:string[];
  query:myTypes.Query | null;

  
  constructor(request:myTypes.ServerBaseRequest)
  {
    this.action = request.action;
    this.path = request.path;
    this.collections = [];
    this.documents = [];
    this.processPath();
    this.query = null;
  }

  protected abstract buildQuery():void;

  public async execute():Promise<myTypes.ServerAnswer>
  {
    if(this.query === null) throw new Error("unable to execute CQL operation, query not built");
    return datastaxTools.runDB(this.query);
  }

  private processPath()
  {
    let names:string[] = this.path.split('/');
    for(let i:number = 0; i<names.length; i++)
    {
      if(i%2 === 0)
      {
        this.collections.push(names[i]);
        let nameCtrl = names[i].match("^[a-z]*$");
        if(nameCtrl === null) throw new Error("Invalid collection name");
      } 
      else
      {
        this.documents.push(names[i]);
        let nameCtrl = names[i].match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
        if(nameCtrl === null) throw new Error("Invalid document ID");
      }      
    }
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
    res = res.slice(0,-5);
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
    return res;
  }

  public async createTable(tableOptions?:myTypes.TableOptions):Promise<void>
  {
    let tableName:string = this.buildTableName();
    let res = "CREATE TABLE IF NOT EXISTS " + tableName + " (";
    let primaryKey:string = "(";
    for(let i:number = 0; i<this.collections.length; i++)
    {
      res = res + this.collections[i] + " uuid, ";
      primaryKey = primaryKey + this.collections[i] + ", ";
    }
    primaryKey = primaryKey.slice(0,-2) + ")";
    res = res + "content text, PRIMARY KEY " + primaryKey + ")";
    await datastaxTools.runDB({query:res, params:[]});
    
    //Add tableOtions
    //TODO
  }
};

class SaveOperation extends BaseOperation
{
  object:myTypes.EncObject;
  options?:myTypes.SaveOptions;

  tableCreationFlag:boolean = false;
  tableOptions?:myTypes.TableOptions;

  constructor(request:myTypes.ServerBaseRequest)
  {
    super(request);
    if(this.documents.length != this.collections.length) throw new Error("Invalid path length parity for a save operation");
    if(request.object === undefined) throw new Error("Missing field object for a save operation")
    this.object = request.object;
    this.options = request.options;
    this.fillObjectKey();
    this.buildQuery();
  }

  protected buildQuery():void
  {
    let tableName:string = super.buildTableName();
    let keys:string = "(";
    let placeholders:string = "(";
    let params:string[] = [];
    Object.entries(this.object).forEach(([key, value]:string[]) => 
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
        return await super.createTable(this.tableOptions)
        .then( () => super.execute());
      }
      return {status:"error", error:err};
    });
  }

  protected fillObjectKey():void
  {
    for(let i:number = 0; i<this.documents.length; i++)
    {
      this.object[this.collections[i]] = this.documents[i];
    }
  }
};

class GetOperation extends BaseOperation
{
  filter?:myTypes.Filter;
  order?:myTypes.Order;
  limit?:number;
  pageToken?:string;

  constructor(request:myTypes.ServerBaseRequest)
  {
    super(request);
    this.filter = request.filter;
    this.order = request.order;
    this.limit = request.limit;
    this.pageToken = request.pageToken;

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

class RemoveOperation extends BaseOperation
{
  constructor(request:myTypes.ServerBaseRequest)
  {
    super(request);
    if(this.documents.length != this.collections.length) throw new Error("Invalid path length parity for a remove operation");
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

class BatchOperation implements myTypes.Operation
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
      await op.createTable(this.tableOptions);
    }
  }
} 

export default function createOperation(request:myTypes.ServerBaseRequest):myTypes.Operation
{
  try
  {
    if(request.path === undefined && request.action != myTypes.action.batch) throw new Error("missing field path in request");
    if(request.action !== myTypes.action.batch) request.path = request.path.toLowerCase();
    let op:BaseOperation;
    switch(request.action)
    {
      case myTypes.action.batch:
      {
        if(request.operations === undefined) throw new Error("missing field operations in batch request");
        let batch = new BatchOperation([]);
        for(let req of request.operations)
        {
          let operation = createOperation(req);
          if(!(operation instanceof BaseOperation)) throw new Error("Only base operations are allowed in a batch");
          batch.push(operation);
        }
        return batch;
      }
      case myTypes.action.save:
      {
        if(request.object == undefined) throw new Error("missing field object in save request");
        op = new SaveOperation(request);
        return op;
      }
      case myTypes.action.get:
      {
        op = new GetOperation(request);
        return op;
      }
      case myTypes.action.remove:
      {
        let op = new RemoveOperation(request);
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
    console.log("Failed to create operation: \n",err);
    throw err;
  }
};