import cassandra from "cassandra-driver";
import { table } from "console";
import { kMaxLength } from "buffer";
//import uuid from "uuid";

const client = new cassandra.Client(
{
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1'
});

client.connect();
let keyspace:string = "excelsir";
let initQuery = "USE " + keyspace;
client.execute(initQuery);


enum action 
{
  save = "save",
  get = "get",
  remove = "remove",
  configure = "configure",
  createTable = "createTable"
}

type ServerBaseRequest = {
  action: action,
  path:string,
  object?:any,
  options?:any,
  filter?:any,
  order?:any,
  limit?:number,
  pageToken?:string
}

type CQLResponseError = {
  name:string,
  info:string,
  message:string,
  code:number,
  query:string
}

type Query = {
  queryTxt:string;
  params:string[];
}

type QueryResult = {
  resultCount:number;
  results:JSON[];
}

type StorageType = {
  [key:string]:string;
  content:any;  
}

type TableOptions = {
 //TODO
}

let defaultTableOptions:TableOptions = {
  //TODO
};

abstract class BaseOperation
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

  public async execute():Promise<QueryResult | CQLResponseError | null>
  {
    if(this.query === null) throw new Error("unable to execute CQL operation, query not built");
    console.log(this.query.queryTxt);
    console.log(this.query.params)
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
    return {queryTxt: res, params: params};
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

  protected async createTable(tableOptions?:TableOptions):Promise<void>
  {
    let tableName:string = this.buildTableName();
    let res = "CREATE TABLE " + tableName + " (";
    let primaryKey:string = "(";
    for(let i:number = 0; i<this.collections.length; i++)
    {
      res = res + this.collections[i] + " uuid, ";
      primaryKey = primaryKey + this.collections[i] + ", ";
    }
    primaryKey = primaryKey.slice(0,-2) + ")";
    res = res + "content text, PRIMARY KEY " + primaryKey + ")";
    console.log(res);
    await runDB({queryTxt:res, params:[]});
    
    //Add tableOtions
    //TODO
  }
}

class SaveOperation extends BaseOperation
{
  object:StorageType;
  options?:string;

  tableCreationFlag:boolean = false;
  tableOptions?:TableOptions;

  constructor(request:ServerBaseRequest)
  {
    super(request);
    if(this.documents.length != this.collections.length) throw new Error("Invalid path length parity for a save operation");
    this.object = request.object;
    this.options = request.options;
    this.fillObjectKey();
    this.buildQuery();
  }

  protected buildQuery():void
  {
    let tableName:string = super.buildTableName();
    this.query = {queryTxt: "INSERT INTO " + tableName + " JSON '" + JSON.stringify(this.object) + "'", params: []};
    if( this.options != undefined)
    {
      //TODO
    }
  }

  public async execute():Promise<QueryResult | CQLResponseError | null>
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
}

class GetOperation extends BaseOperation
{
  filter?:JSON;
  order?:string;
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
      queryTxt: "SELECT JSON * FROM " + tableName + " WHERE " + whereClause.queryTxt ,
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
}

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
      queryTxt: "DELETE FROM " + tableName + " WHERE " + whereClause.queryTxt ,
      params: whereClause.params 
    };
  }
}

/* class batchOperation extends BaseOperation
{
  //TODO
}
 */

async function runDB(query:Query):Promise<QueryResult | null>
{
  let rs:any;
  rs = await client.execute(query.queryTxt, query.params, {prepare: true});
  const ans = rs.first();
  console.log(rs);
  if(ans == null)
  {
    return null;
  }
  let result:QueryResult = {resultCount:rs.rowLength, results:[]};
  for(let i:number = 0; i<result.resultCount; i++)
  {
    result.results.push(JSON.parse(rs.rows[i]['[json]']));
  }
  console.log("Result =",result);
  return result;
}



export default function createOperation(request:ServerBaseRequest):BaseOperation
{
  try
  {
    console.log(request.action);
    if(request.path == undefined) throw new Error("missing field path in request");
    request.path = request.path.toLowerCase();
    let op:BaseOperation;
    switch(request.action)
    {
      case action.save:
      {
        if(request.object == undefined) throw new Error("missing field object in save request");
        op = new SaveOperation(request);
        return op;
      }
      case "get":
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
}