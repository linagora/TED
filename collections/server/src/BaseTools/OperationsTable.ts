import * as myTypes from "./myTypes";
import * as CQL from "./BaseOperations";

export class OperationLog extends CQL.BaseOperation
{
    op_id:myTypes.OperationID | null;
    entry:myTypes.LogEntry;

    constructor(operation:myTypes.InternalOperationDescription)
    {
        super({
            action: myTypes.action.log,
            collections: operation.collections,
            documents: operation.documents,
            tableOptions:{secondaryTable:false}
        });
        this.op_id = null;
        this.entry = {log: this.createLog(operation)};
        this.buildEntry();
        this.buildQuery();
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
      return await super.execute()
      .catch(async (err:myTypes.CQLResponseError) =>
      {
        if(err.code === 8704 && err.message.substr(0,18) === "unconfigured table")
        {
          return await this.createTable()
          .then( () => super.execute());
        }
        return {status:"error", error:err};
      });
    }
  

    protected buildTableName():string
    {
        return super.buildTableName() + "__operations";
    }

    protected buildQuery():void
    {
        let tableName:string = this.buildTableName();
        let params:string[] = [];
        let keys:string = "(op_id, ";
        let placeholders:string = "(now(), ";
        Object.entries(this.entry).forEach(([key, value]:string[]) => 
        {
          keys = keys + key + ", ";
          placeholders = placeholders + "? , ";
          params.push(value)
        })
        keys = keys.slice(0,-2) + ")";
        placeholders = placeholders.slice(0,-2) + ")";
        this.query = {query: "INSERT INTO " + tableName + " " + keys + " VALUES " + placeholders, params: params};
    }
    
    protected buildEntry():void
    {
        for(let i:number = 0; i<this.collections.length - 1; i++)
        {
          this.entry[this.collections[i]] = this.documents[i];
        }
    }

    protected createLog(operation:myTypes.InternalOperationDescription):string
    {
      let copy:myTypes.InternalOperationDescription = {...operation};
      delete copy.clearObject;
      return JSON.stringify(copy);
    }

    public async createTable():Promise<void>
    {
        let tableDefinition:myTypes.TableDefinition = {
            name: this.buildTableName(),
            keys : ["op_id", "log"],
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
        return await CQL.BaseOperation.createTable(tableDefinition);
    }
}
