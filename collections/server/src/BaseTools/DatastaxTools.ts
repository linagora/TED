import cassandra from "cassandra-driver";
import * as myTypes from "./myTypes";
import { v4 as uuidv4 } from "uuid";
import * as config from "../Config/config";
import { globalCounter } from "./../index";
import { Timer, RequestTracker } from "./../Monitoring/Timer";

export const client = new cassandra.Client(
{
    contactPoints: config.cassandraContactPoint,
    localDataCenter: config.cassandraLocalDatacenter,
    keyspace: config.cassandraKeyspace,
    policies: {
      retry: new cassandra.policies.retry.IdempotenceAwareRetryPolicy(new cassandra.policies.retry.RetryPolicy())  
    },
    queryOptions: {
      isIdempotent: true
    }
});
 
export async function setup():Promise<void>
{
  await client.connect()
  .catch( async (err:myTypes.CQLResponseError) => 
  {
      if( err.code === 8704 && err.message.match("^Keyspace \'.*\' does not exist$"))
      {
      return await createKeyspace(config.cassandraKeyspace, config.defaultCassandraKeyspaceOptions as myTypes.KeyspaceReplicationOptions);
      }
  })
  .then( () => client.connect());
}

const defaultQueryOptions:myTypes.QueryOptions = {
    keyspace:config.cassandraKeyspace,
    prepare:true,
}

export async function runDB(query:myTypes.Query, options?:myTypes.QueryOptions):Promise<myTypes.ServerAnswer>
{
  globalCounter.inc("single CQL request");
  let timer = new Timer("single CQL request");
  let queryID = uuidv4()
  console.log("Begin query "+ queryID + ",\n   " + JSON.stringify(query) );
  try
  {
    let rs:any;
    if(options === undefined) options = defaultQueryOptions;
    rs = await client.execute(query.query, query.params, options);
    console.log("   End query ", queryID);
    timer.stop();
    return processResult(rs);
  }
  catch(err)
  {
    console.log("   Error thrown by query ", queryID, "  :  ", err.message);
    timer.stop();
    throw err;
  }
};

export async function runBatchDB(queries:myTypes.Query[], options?:myTypes.QueryOptions, tracker?:RequestTracker):Promise<myTypes.ServerAnswer>
{
  globalCounter.inc("CQL batch");
  let timer = new Timer("CQL batch");
  let queryStr:string[] = queries.map( (value:myTypes.Query) => JSON.stringify(value))
  let queryID = uuidv4()
  console.log("Begin query "+ queryID + ",\n  ", queryStr.join(";\n   ") );
  try
  {
    let rs:any;
    if(options === undefined) options = defaultQueryOptions;
    rs = await client.batch(queries, options);
    console.log("   End query ", queryID);
    timer.stop();
    tracker?.endStep("Batch write");
    let res = processResult(rs);
    tracker?.endStep("result computation");
    return res;
  }
  catch(err)
  {
    console.log("   Error thrown by query ", queryID, "  :  ", err.message);
    timer.stop();
    throw err;
  }
}

function processResult(rs:any):myTypes.ServerAnswer
{
  const ans = rs.first();
  if(ans == null)
  {
    return {status: "success", queryResults:{resultCount:0, allResultsEnc:[], allResultsClear:[]}};
  }
  let queryResults:myTypes.QueryResult = {resultCount:rs.rowLength};
  queryResults.allResultsEnc = [];
  queryResults.allResultsClear = [];
  for(let i:number = 0; i<queryResults.resultCount; i++)
  {
    let object = JSON.parse(rs.rows[i]['[json]']);
    try
    {
      JSON.parse(object["object"]);
      queryResults.allResultsEnc.push(object);
    }
    catch
    {
      queryResults.allResultsClear.push(object);
    }
  }
  return {status: "success", queryResults:queryResults};
}

export async function createKeyspace(keyspaceName:string, options:myTypes.KeyspaceReplicationOptions):Promise<void>
{
  let nameCtrl = keyspaceName.match(/^[a-zA-Z\_]*$/);
  if(nameCtrl === null) throw new Error("Invalid keyspace name");
  let res = "CREATE KEYSPACE " + keyspaceName + " WITH replication = " + JSON.stringify(options);
  res = res.split('"').join("'");
  let clientTemp = new cassandra.Client({
    contactPoints: config.cassandraContactPoint,
    localDataCenter: config.cassandraLocalDatacenter,
  });
  console.log(res);
  await clientTemp.execute(res);
};