import cassandra, { types } from "cassandra-driver";
import { readFileSync } from "fs";
import * as myTypes from "../../../utils/myTypes";
import { v4 as uuidv4 } from "uuid";
import * as config from "../../../../../config/config";
import { globalCounter } from "../../../../index";
import { Timer, RequestTracker } from "../../../monitoring/Timer";

let cassandraOptions:cassandra.DseClientOptions;
export let client:cassandra.Client;
 
export async function setup():Promise<void>
{
  switch(config.cassandra.core)
  {
    case "keyspace":
    {
      const auth = new cassandra.auth.PlainTextAuthProvider(config.cassandra.keyspaceID, config.cassandra.keyspaceKey);
      const sslOptions = {
        ca: [readFileSync("src/config/AmazonRootCA1.pem", 'utf-8')],
        host: config.cassandra.contactPoint[0],
        rejectUnauthorized: true
      };
      cassandraOptions = {
        contactPoints: config.cassandra.contactPoint,
        localDataCenter: config.cassandra.localDatacenter,
        keyspace: config.cassandra.keyspace,
        policies: {
          retry: new cassandra.policies.retry.IdempotenceAwareRetryPolicy(new cassandra.policies.retry.RetryPolicy())  
        },
        queryOptions: {
          isIdempotent: true
        },
        authProvider: auth,
        sslOptions: sslOptions,
        protocolOptions: {port: 9142}
      };
      break;
    }
    case "scylladb":
    case "cassandra":
    {
      cassandraOptions = {
        contactPoints: config.cassandra.contactPoint,
        localDataCenter: config.cassandra.localDatacenter,
        keyspace: config.cassandra.keyspace,
        policies: {
          retry: new cassandra.policies.retry.IdempotenceAwareRetryPolicy(new cassandra.policies.retry.RetryPolicy())  
        },
        queryOptions: {
          isIdempotent: true
        }
      };
      break;
    }
    default:
    {
      throw new Error("Unsupported DB core");
    }
  }
  client = new cassandra.Client(cassandraOptions);
  await client.connect()
  .catch( async (err:myTypes.CQLResponseError) => 
  {
      if( err.code === 8704 && err.message.match("^Keyspace \'.*\' does not exist$"))
      {
        console.error(err);
        console.log("trying to create keyspace");
        return await createKeyspace(config.cassandra.keyspace, config.cassandra.defaultCassandraKeyspaceOptions as myTypes.KeyspaceReplicationOptions);
      }
  })
  .then( () => client.connect());
}



const defaultQueryOptions:cassandra.QueryOptions = {
  prepare: true,
  consistency: types.consistencies.localQuorum
}

export async function runDB(query:myTypes.Query, options?:myTypes.QueryOptions):Promise<myTypes.ServerAnswer>
{
  globalCounter.inc("single_cql_request");
  let timer = new Timer("single_cql_request");
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

export async function runMultiOpDB(queries:myTypes.Query[], options?:myTypes.QueryOptions, tracker?:RequestTracker):Promise<myTypes.ServerAnswer>
{
  let queryStr:string[] = queries.map( (value:myTypes.Query) => JSON.stringify(value))
  let queryID = uuidv4()
  console.log("Begin query "+ queryID + ",\n  ", queryStr.join(";\n   ") );
  try
  {
    if(options === undefined) options = defaultQueryOptions;
    let promises:Promise<unknown>[] = [];
    for(let query of queries)
    {
      promises.push(client.execute(query.query, query.params, options));
    }
    await Promise.all(promises);
    console.log("   End query ", queryID);
    return {status: "success"};
  }
  catch(err)
  {
    console.log("   Error thrown by query ", queryID, "  :  ", err.message);
    throw err;
  }
}

export async function runBatchDB(queries:myTypes.Query[], options?:myTypes.QueryOptions, tracker?:RequestTracker):Promise<myTypes.ServerAnswer>
{
  let queryStr:string[] = queries.map( (value:myTypes.Query) => JSON.stringify(value))
  let queryID = uuidv4()
  console.log("Begin query "+ queryID + ",\n  ", queryStr.join(";\n   ") );
  try
  {
    if(options === undefined) options = defaultQueryOptions;
    await client.batch(queries, options);
    console.log("   End query ", queryID);
    return {status: "success"};
  }
  catch(err)
  {
    console.log("   Error thrown by query ", queryID, "  :  ", err.message);
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
  let optionsTemp = {...cassandraOptions};
  delete optionsTemp.keyspace;
  let clientTemp = new cassandra.Client(optionsTemp);
  await clientTemp.execute(res);
  console.log("keyspace created");
};