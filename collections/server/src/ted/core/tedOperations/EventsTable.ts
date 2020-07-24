import * as myTypes from "../../services/utils/myTypes";
import { createTable } from "../../services/database/adapters/cql/TableCreation";
import { SaveOperation, tableCreationError } from "../../services/database/operations/baseOperations";

export class SaveEventStore extends SaveOperation
{
  constructor(operation:myTypes.InternalOperationDescription)
  {
    super({
      action: myTypes.action.save,
      opID: operation.opID,
      collections: operation.collections,
      documents: operation.documents,
      encObject: SaveEventStore.createLog(operation)
    });
    this.canCreateTable = true;
    this.table = this.buildTableName();
    this.buildOperation();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    return await super.execute()
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
      {
        await this.createTable();
        throw tableCreationError;
      }
      return {status:"error", error:err};
    });
  }
  

  public buildTableName():string
  {
    return super.buildTableName() + "__events";
  }

  protected static createLog(operation:myTypes.InternalOperationDescription):string
  {
    let copy:myTypes.InternalOperationDescription = {...operation};
    delete copy.clearObject;
    return JSON.stringify(copy);
  }

  protected buildEntry():myTypes.DBentry
  {
    try
    {
      let entry:myTypes.DBentry = {};
      for(let i:number = 0; i < this.documents.length - 1; i++)
      {
        entry[this.collections[i]] = this.documents[i];
      }
      entry["op_id"] = this.opID;
      entry["object"] = this.object;
      return entry;
    }
    catch(err)
    {
      throw new Error("Wrong collection/document arguments in operation :" + err);
    }
  }

  public async createTable():Promise<void>
  {
    let tableDefinition:myTypes.TableDefinition = {
      name: this.buildTableName(),
      keys : ["op_id", "object"],
      types : ["timeuuid", "text"],
      primaryKey: []
    }
    for(let i:number = 0; i<this.collections.length - 1; i++)
    {
      tableDefinition.keys.push(this.collections[i]);
      tableDefinition.primaryKey.push(this.collections[i]);
      tableDefinition.types.push("uuid");
    }
    tableDefinition.primaryKey.push("op_id");
    return createTable(tableDefinition);
  }
}
