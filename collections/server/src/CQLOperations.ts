import cassandra from "cassandra-driver";
import { table } from "console";
import { kMaxLength } from "buffer";
import { Serializer } from "v8";
//import uuid from "uuid";

const client = new cassandra.Client(
{
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1'
});

const defaultKeyspaceOptions:KeyspaceReplicationOptions = {
  class:"SimpleStrategy",
  replication_factor:3
};

client.connect();
let keyspace:string = "twake_collections";
let initQuery = "USE " + keyspace;
client.execute(initQuery)
.catch( async (err:CQLResponseError) => 
{
  if( err.code === 8704 && err.message.match("^Keyspace \'.*\' does not exist$"))
  {
    return await createKeyspace(keyspace, defaultKeyspaceOptions).then( () => client.execute(initQuery));
  }
});

enum action 
{
  save = "save",
  get = "get",
  remove = "remove",
  configure = "configure",
  batch = "batch"
};

type QueryOptions = cassandra.QueryOptions;

type KeyspaceReplicationOptions = {
  class:"SimpleStrategy" | "NetworkTopologyStrategy" | "OldNetworkTopologyStrategy";
  replication_factor?:number;
  datacentersRF?:JSON;
};

type SaveOptions = {
  ttl?:number;
};

type Filter = {
  //TODO
};

type Order = {
  //TODO
};

type ServerBaseRequest = {
  action: action;
  path:string;
  object?:StorageType;
  options?:SaveOptions | any;
  filter?:Filter;
  order?:Order;
  limit?:number;
  pageToken?:string;
  operations?:ServerBaseRequest[];
};

type CQLResponseError = {
  name:string;
  info:string;
  message:string;
  code:number;
  query:string;
};

type Query = {
  query:string;
  params:string[];
};

const defaultQueryOptions:QueryOptions = {
  keyspace:keyspace,
  prepare:true,

}

type QueryResult = {
  resultCount:number;
  results:JSON[];
};

type EmptyResult = {
  status:string;
}

type StorageType = {
  [key:string]:string;
  content:any;  
};

type TableOptions = {
 //TODO
};

let defaultTableOptions:TableOptions = {
  //TODO
};

interface Operation 
{
  action:action;
  execute():Promise<QueryResult | CQLResponseError | EmptyResult>;
}

abstract class BaseOperation implements Operation
{
  action:action;
  path:string;
  collections:string[];
  documents:string[];
  query:Query | null;

  
  constructor(request:ServerBaseRequest)
  {
    this.action = request.action;
    this.path = request.path;
    this.collections = [];
    this.documents = [];
    this.processPath();
    this.query = null;
  }

  protected abstract buildQuery():void;

  public async execute():Promise<QueryResult | CQLResponseError | EmptyResult>
  {
    if(this.query === null) throw new Error("unable to execute CQL operation, query not built");
    return runDB(this.query);
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

  protected buildPrimaryKey():Query
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

  public async createTable(tableOptions?:TableOptions):Promise<void>
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
    console.log(res);
    await runDB({query:res, params:[]});
    
    //Add tableOtions
    //TODO
  }
};

class SaveOperation extends BaseOperation
{
  object:StorageType;
  options?:SaveOptions;

  tableCreationFlag:boolean = false;
  tableOptions?:TableOptions;

  constructor(request:ServerBaseRequest)
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

  public async execute():Promise<QueryResult | CQLResponseError | EmptyResult>
  {
    return await super.execute()
    .catch(async (err:CQLResponseError) =>
    {
      if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
      {
        this.tableCreationFlag = true;
        return await super.createTable(this.tableOptions)
        .then( () => super.execute());
      }
      return err;
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
  filter?:Filter;
  order?:Order;
  limit?:number;
  pageToken?:string;

  constructor(request:ServerBaseRequest)
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
    let whereClause:Query = super.buildPrimaryKey();
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
  constructor(request:ServerBaseRequest)
  {
    super(request);
    if(this.documents.length != this.collections.length) throw new Error("Invalid path length parity for a remove operation");
    this.buildQuery();
  }

  protected buildQuery():void
  {
    let tableName:string = super.buildTableName();
    let whereClause:Query = super.buildPrimaryKey();
    this.query = {
      query: "DELETE FROM " + tableName + " WHERE " + whereClause.query ,
      params: whereClause.params 
    };
  }
};

class BatchOperation implements Operation
{
  action:action;
  operations:BaseOperation[];
  queries:Query[] | null;

  options?:QueryOptions;

  tableCreationFlag:boolean = false;
  tableOptions?:TableOptions;

  constructor(batch:BaseOperation[])
  {
    this.action = action.batch;
    this.operations = batch;
    for(let op of this.operations)
    {
      if(op.action === action.batch || op.action === action.get) throw new Error("Batch cannot contain batch or get operations");
    }
    this.queries = null;
  }

  public async execute():Promise<QueryResult | CQLResponseError | EmptyResult>
  {
    this.buildQueries();
    if(this.queries === null) throw new Error("Error in batch, invalid query");
    return runBatchDB(this.queries)
    .catch(async (err:CQLResponseError) =>
    {
      if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
      {
        this.tableCreationFlag = true;
        return await this.createAllTables()
        .then( async () => 
        {
          if(this.queries === null) throw new Error("Error in batch, invalid query");
          return await runBatchDB(this.queries);
        });
      }
      return err;
    });
  }

  public push(operation:BaseOperation)
  {
    if(operation.action === action.batch || operation.action === action.get) throw new Error("Batch cannot contain batch or get operations");
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

async function runDB(query:Query, options?:QueryOptions):Promise<QueryResult | EmptyResult>
{
  console.log(query);
  let rs:any;
  if(options === undefined) options = defaultQueryOptions;
  rs = await client.execute(query.query, query.params, options);
  return processResult(rs);
};

async function runBatchDB(queries:Query[], options?:QueryOptions):Promise<QueryResult | EmptyResult>
{
  console.log(queries);
  let rs:any;
  if(options === undefined) options = defaultQueryOptions;
  rs = await client.batch(queries, options);
  return processResult(rs);
}

function processResult(rs:any):QueryResult | EmptyResult
{
  const ans = rs.first();
  console.log(rs.info);
  if(ans == null)
  {
    return {status: "Query successful. No result to display"};
  }
  let result:QueryResult = {resultCount:rs.rowLength, results:[]};
  for(let i:number = 0; i<result.resultCount; i++)
  {
    result.results.push(JSON.parse(rs.rows[i]['[json]']));
  }
  console.log("Result =",result);
  return result;
}

async function createKeyspace(keyspaceName:string, options:KeyspaceReplicationOptions):Promise<void>
{
  let nameCtrl = keyspaceName.match(/^[a-zA-Z\_]*$/);
  if(nameCtrl === null) throw new Error("Invalid keyspace name");
  let res = "CREATE KEYSPACE " + keyspaceName + " WITH replication = " + JSON.stringify(options);
  res = res.split('"').join("'");
  console.log(res);
  await runDB({query:res, params:[]});
};


export default function createOperation(request:ServerBaseRequest):Operation
{
  try
  {
    console.log(request.action);
    if(request.path === undefined && request.action != action.batch) throw new Error("missing field path in request");
    if(request.action !== action.batch) request.path = request.path.toLowerCase();
    let op:BaseOperation;
    switch(request.action)
    {
      case action.batch:
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
      case action.save:
      {
        if(request.object == undefined) throw new Error("missing field object in save request");
        op = new SaveOperation(request);
        return op;
      }
      case action.get:
      {
        op = new GetOperation(request);
        return op;
      }
      case action.remove:
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