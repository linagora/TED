import { TaskTable } from "../../../utils/antiDuplicata";
import * as myTypes from "../../../utils/myTypes";
import { delay } from "../../../utils/divers";
import { globalCounter } from "../../../../index";
import { Timer } from "../../../monitoring/Timer";
import * as datastaxTools from "./DatastaxTools";
import { cassandra, ted } from "../../../../../config/config";

let runningTableCreation = new TaskTable(ted.maxTableCreation);

export async function createTable(tableDefinition:myTypes.TableDefinition):Promise<void>
{
  tableDefinition.name = cassandra.keyspace + "." + tableDefinition.name;
  try{
    if(runningTableCreation.isDone(tableDefinition.name)) 
    {
      console.log("table already created");
      await delay(5000);
      return;
    }
    if(runningTableCreation.isRunning(tableDefinition.name))
    {
      console.log("Waiting for creation of table ", tableDefinition.name);
      await runningTableCreation.waitTask(tableDefinition.name);
      return;
    }
    await runningTableCreation.pushTask(tableDefinition.name);
    await createTableRetry(tableDefinition);
    runningTableCreation.endTask(tableDefinition.name);
  }
  catch(err){
    runningTableCreation.failTask(tableDefinition.name);
    throw err;
  }

}

export async function createTableRetry(tableDefinition:myTypes.TableDefinition):Promise<void>
{
  let tableTimer = new Timer("table_creation");
  let query = createTableQuery(tableDefinition);
  await datastaxTools.runDB(query, {})
  .catch( async (err) => 
  {
    await delay(1000);
    await datastaxTools.runDB(query, {});
  })
  .catch( async (err) => 
  {
    await delay(2000);
    await datastaxTools.runDB(query, {});
  })
  .catch( async (err) => 
  {
    await delay(5000);
    await datastaxTools.runDB(query, {});
  })
  .catch( async (err) => 
  {
    await delay(10000);
    await datastaxTools.runDB(query, {});
  });
  tableTimer.stop();
}

function createTableQuery(tableDefinition:myTypes.TableDefinition):myTypes.Query
{
  globalCounter.inc("tables_ceated");
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
  return {query: res, params:[]};
}
