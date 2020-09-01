import * as myTypes from "../../services/utils/myTypes";
import { createTable } from "../../services/database/operations/baseOperations";
import { SaveOperation, GetOperation, tableCreationError, RemoveOperation } from "../../services/database/operations/baseOperations";

/**
 * Represents a save operation on a MainView.
 * 
 * Writes an encrypted object on a MainView table. Creates the table if necessary.
 * 
 * @constructs SaveMainView
 * @augments SaveOperation
 * 
 * @param {myTypes.InternalOperationDescription} request the description of the save operation.
 */
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
    let res = await super.execute()
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      //If the table doesn't exist, creates it and throws an error (cancel the operation, it will be retried after the table creation).
      if((err.code === 8704 && err.message.substr(0,18) === "unconfigured table") || err.message.match(/^Collection ([a-zA-z_]*) does not exist./))
      {
        await this.createTable();
        throw tableCreationError;
      }
      console.error(err);
      return {status:"error", error:err};
    });
    this.done();
    return res;
  }

  public done():void { console.log("---MainView write OK"); }

  public buildTableName():string
  {
    return super.buildTableName() + "__mainview";
  }

  /**
   * Creates the MainView table.
   * 
   * Initializes a table with these fields :
   * - [collection_names] : uuid;
   * - object : text;
   * 
   * The primary key is ( [collection_names]:uuid ).
   * 
   * @returns {Promise<void>} Resolves when the table is created (except for Keyspace, whose tables are created asynchronously).
   */
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

/**
 * Represents a Get operation a MainView table.
 * 
 * Reads the complete projection of one or many documents. Cannot operate any filtering on the content of the documents as they are encrypted.
 *  
 * @constructs GetMainView
 * @augments GetOperation
 * 
 * @param {myTypes.InternalOperationDescription} request the operation description.
 * @param {string} [pageToken] an optionnal pageToken that bypass the one computed by the DB core.
 */
export class GetMainView extends GetOperation
{
  
  constructor(request:myTypes.InternalOperationDescription, pageToken?:string)
  {
    super(request, pageToken);
    this.table = this.buildTableName();
    this.buildOperation();
  }

  public async execute():Promise<myTypes.ServerAnswer>
  {
    let res = await super.execute();
    this.done();
    console.log(res);
    return res;
  }

  public done():void { console.log("---MainView read OK"); }

  public buildTableName():string
  {
    return super.buildTableName() + "__mainview";
  }

  public async createTable():Promise<void>{}
};

/**
 * Represents a remove operation on a MainView.
 * 
 * @constructs RemoveMainView
 * @augments RemoveOperation
 * 
 * @param {myTypes.InternalOperationDescription} request the description of the remove operation.
 */
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
    this.done();
    return res;
  }

  public done():void { console.log("---MainView remove OK"); }

  public buildTableName():string
  {
    return super.buildTableName() + "__mainview";
  }

  public async createTable():Promise<void>{}
};

/**
 * Computes an object previous value.
 * 
 * If an object already exist in the DB, finds it and return its value (encrypted). Else returns null.
 * 
 * @param {myTypes.InternalOperationDescription} opDescriptor the operation that needs to know the previous value of the object.
 * 
 * @returns {myTypes.EncObject | null} null if the object doesn't exist, its value otherwise.
 */
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