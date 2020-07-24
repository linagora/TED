import * as myTypes from "../../services/utils/myTypes";
import { createTable } from "../../services/database/adapters/cql/TableCreation";
import { SaveOperation, GetOperation, tableCreationError, RemoveOperation } from "../../services/database/operations/baseOperations";

export class SaveMainView extends SaveOperation
{
  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    this.canCreateTable = true;
    this.table = this.buildTableName();
    this.buildOperation();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    return super.execute()
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
    return super.buildTableName() + "__mainview";
  }

  public async createTable():Promise<void>
  {
    let tableDefinition:myTypes.TableDefinition = {
      name: this.buildTableName(),
      keys: [],
      types: [],
      primaryKey: []
    };
    for(let i:number = 0; i<this.documents.length; i++)
    {
      tableDefinition.keys.push(this.collections[i]);
      tableDefinition.primaryKey.push(this.collections[i]);
      tableDefinition.types.push("uuid");
    }
    tableDefinition.keys.push("object");
    tableDefinition.types.push("text");
    return createTable(tableDefinition);
  }
};

export class GetMainView extends GetOperation
{
  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    this.table = this.buildTableName();
    this.buildOperation();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    let res = await super.execute();
    return res;
  }

  public buildTableName():string
  {
    return super.buildTableName() + "__mainview";
  }

  public async createTable():Promise<void>{}
};

export class RemoveMainView extends RemoveOperation
{
  constructor(request:myTypes.InternalOperationDescription)
  {
    super(request);
    this.table = this.buildTableName();
    this.buildOperation();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    let res = await super.execute();
    return res;
  }

  public buildTableName():string
  {
    return super.buildTableName() + "__mainview";
  }

  public async createTable():Promise<void>{}
};

export async function getPreviousValue(opDescriptor:myTypes.InternalOperationDescription):Promise<myTypes.EncObject | null>
{
    if( opDescriptor.collections.length !== opDescriptor.documents.length) throw new Error("Need the object ID to check its previuous value");
    let getter:GetMainView = new GetMainView({
        action: myTypes.action.get,
        opID: opDescriptor.opID,
        collections: opDescriptor.collections,
        documents: opDescriptor.documents,
    });
    let DBanswer = await getter.execute();
    if(DBanswer.queryResults === undefined || DBanswer.queryResults.allResultsEnc === undefined || DBanswer.queryResults.allResultsEnc.length === 0) return null;
    return JSON.parse(DBanswer.queryResults.allResultsEnc[0].object);
}