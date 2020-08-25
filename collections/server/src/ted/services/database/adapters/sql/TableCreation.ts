import { TaskTable } from "../../../utils/antiDuplicata";
import * as myTypes from "../../../utils/myTypes";
import { delay } from "../../../utils/divers";
import * as mongoDBTools from "./MongoDBtools";
import { globalCounter } from "../../../../index";
import { Timer } from "../../../monitoring/Timer";
import { table } from "console";

let runningTableCreation = new TaskTable();

export async function createTable(tableDefinition:myTypes.TableDefinition):Promise<void>
{
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
  let [collection, index] = createTableQuery(tableDefinition);
  await runCreateTable(collection, index)
  .catch( async (err) => 
  {
    await delay(1000);
    await runCreateTable(collection, index);
  })
  .catch( async (err) => 
  {
    await delay(2000);
    await runCreateTable(collection, index);
  })
  .catch( async (err) => 
  {
    await delay(5000);
    await runCreateTable(collection, index);
  })
  .catch( async (err) => 
  {
    await delay(10000);
    await runCreateTable(collection, index);
  });
  tableTimer.stop();
}

function createTableQuery(tableDefinition:myTypes.TableDefinition):[string, any]
{
  globalCounter.inc("tables_ceated");
  let index:any = {};
  for(let key of tableDefinition.primaryKey)
  {
    index[key] = 1;
  }
  return [tableDefinition.name, index];
}

async function runCreateTable(collection:string, index:any):Promise<void>
{
  console.log("creating table : ", collection);
  let coll = mongoDBTools.database.collection(collection);
  await coll.createIndex(index);
}