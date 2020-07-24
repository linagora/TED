import * as myTypes from "../../../utils/myTypes";
import { buildPath } from "../../../utils/divers";
import { ted, cassandra } from "../../../../../Config/config";
import { createTable } from "../../adapters/cql/TableCreation";
import { SaveOperation, tableCreationError, GetOperation, RemoveOperation } from "../baseOperations";

export class SaveTaskStore extends SaveOperation
{
    constructor(operation:myTypes.InternalOperationDescription)
    {
        super({
            action: myTypes.action.save,
            opID: operation.opID,
            collections: operation.collections,
            documents: operation.documents,
            options: operation.options,
            encObject: SaveTaskStore.createLog(operation)
        });
        if(this.options === undefined) this.options = {};
        //this.options.ttl = ted.defaultTaskStoreTTL;
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
            entry["path"] = buildPath(this.collections, this.documents, true);
            entry["op_id"] = this.opID;
            entry["object"] = this.object;
            return entry;
        }
        catch(err)
        {
            throw new Error("Wrong collection/document arguments in operation :" + err);
        }
    }

    public buildTableName():string
    {
        return cassandra.keyspace + ".global_taskstore";
    }

    public async createTable():Promise<void>
    {
        let tableDefinition:myTypes.TableDefinition = {
            name: this.buildTableName(),
            keys : ["path", "op_id", "object"],
            types : ["text", "timeuuid", "text"],
            primaryKey: ["path", "op_id"]
        }
        return await createTable(tableDefinition);
    }
};

export class GetTaskStore extends GetOperation
{
    keyOverride:myTypes.DBentry;

    constructor(operation:myTypes.InternalOperationDescription)
    {
        super(operation);
        if(operation.keyOverride === undefined) this.keyOverride = {
            path: buildPath(this.collections, this.documents, true)
        };
        else this.keyOverride = operation.keyOverride;
        this.table = this.buildTableName();
        this.buildOperation();
    }

    public buildTableName():string
    {
        return cassandra.keyspace + ".global_taskstore";
    }

    protected buildEntry():myTypes.DBentry
    {
        return this.keyOverride;
    }

    public async createTable():Promise<void>{}
};

export class RemoveTaskStore extends RemoveOperation
{
    keyOverride:myTypes.DBentry;

    constructor(operation:myTypes.InternalOperationDescription)
    {
        super(operation);
        if(operation.keyOverride === undefined) throw new Error("Missing information to delete operation from TaskStore");
        this.keyOverride = operation.keyOverride;
        this.table = this.buildTableName();
        this.buildOperation();
    }

    public buildTableName():string
    {
        return cassandra.keyspace + ".global_taskstore";
    }

    protected buildEntry():myTypes.DBentry
    {
        return this.keyOverride;
    }

    public async createTable():Promise<void>{}
}
