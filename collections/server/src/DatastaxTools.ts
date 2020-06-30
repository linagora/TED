import cassandra from "cassandra-driver";
import * as myTypes from "./myTypes";
import { Server } from "http";

const client = new cassandra.Client(
{
    contactPoints: ['127.0.0.1'],
    localDataCenter: 'datacenter1'
});
  
client.connect();
let keyspace:string = "twake_collections";
let initQuery = "USE " + keyspace;
client.execute(initQuery)
.catch( async (err:myTypes.CQLResponseError) => 
{
    if( err.code === 8704 && err.message.match("^Keyspace \'.*\' does not exist$"))
    {
    return await createKeyspace(keyspace, defaultKeyspaceOptions).then( () => client.execute(initQuery));
    }
});

const defaultQueryOptions:myTypes.QueryOptions = {
    keyspace:keyspace,
    prepare:true,
}

const defaultKeyspaceOptions:myTypes.KeyspaceReplicationOptions = 
{
    class:"SimpleStrategy",
    replication_factor:3
};

export async function runDB(query:myTypes.Query, options?:myTypes.QueryOptions):Promise<myTypes.ServerAnswer>
{
  console.log("Query =\n",query);
  let rs:any;
  if(options === undefined) options = defaultQueryOptions;
  rs = await client.execute(query.query, query.params, options);
  return processResult(rs);
};

export async function runBatchDB(queries:myTypes.Query[], options?:myTypes.QueryOptions):Promise<myTypes.ServerAnswer>
{
  console.log("Queries =\n",queries);
  let rs:any;
  if(options === undefined) options = defaultQueryOptions;
  rs = await client.batch(queries, options);
  return processResult(rs);
}

function processResult(rs:any):myTypes.ServerAnswer
{
  const ans = rs.first();
  if(ans == null)
  {
    return {status: "success", queryResults:{resultCount:0, allResultsEnc:[]}};
  }
  let queryResults:myTypes.QueryResult = {resultCount:rs.rowLength};
  queryResults.allResultsEnc = [];
  for(let i:number = 0; i<queryResults.resultCount; i++)
  {
    queryResults.allResultsEnc.push(JSON.parse(rs.rows[i]['[json]']));
  }
  console.log("Result =\n",queryResults);
  return {status: "success", queryResults:queryResults};
}

export async function createKeyspace(keyspaceName:string, options:myTypes.KeyspaceReplicationOptions):Promise<void>
{
  let nameCtrl = keyspaceName.match(/^[a-zA-Z\_]*$/);
  if(nameCtrl === null) throw new Error("Invalid keyspace name");
  let res = "CREATE KEYSPACE " + keyspaceName + " WITH replication = " + JSON.stringify(options);
  res = res.split('"').join("'");
  await runDB({query:res, params:[]});
};