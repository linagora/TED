import * as myTypes from "./myTypes";
import * as CQL from "./BaseOperations";
import {buildPath} from "./../MacroRoutines/RequestHandling";

export class SaveTaskStore extends CQL.BaseOperation
{
    entry:myTypes.LogEntry;
    ttl:number = 3600;

    constructor(operation:myTypes.InternalOperationDescription)
    {
        super({
            action: myTypes.action.log,
            opID: operation.opID,
            collections: operation.collections,
            documents: operation.documents,
            tableOptions:{secondaryTable:false}
        });
        this.entry = {object: this.createLog(operation)};
        this.canCreateTable = true;
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
        return "global_taskstore";
    }

    protected buildQuery():void
    {
        let tableName:string = this.buildTableName();
        let params:string[] = [buildPath(this.collections, this.documents.slice(0,-1)), this.opID, this.entry.object];
        let keys:string = "(path, op_id, object)";
        let placeholders:string = "(?, ?, ?)";
        this.query = {query: "INSERT INTO " + tableName + " " + keys + " VALUES " + placeholders + " USING TTL " + this.ttl, params: params};
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
            keys : ["path", "op_id", "object"],
            types : ["text", "timeuuid", "text"],
            primaryKey: ["path", "op_id"]
        }
        return await CQL.createTable(tableDefinition);
    }
};

export class GetTaskStore extends CQL.GetOperation
{
    keyOverride:myTypes.DBentry;

    constructor(request:myTypes.InternalOperationDescription)
    {
        super(request);
        if(request.keyOverride === undefined) this.keyOverride = {};
        else this.keyOverride = request.keyOverride;
        this.buildQuery();
    }

    protected buildTableName():string
    {
        return "global_taskstore";
    }

    protected buildPrimaryKey():myTypes.Query
    {
        if(this.keyOverride === undefined) return {query:"", params:[]};
        let res = "";
        let params:string[] = [];
        Object.entries(this.keyOverride).forEach( ([key, value]) => 
        {
            res = res + key + " = ? AND ";
            params.push(value);
        });
        res = res.slice(0,-5);
        return {query: res, params: params};
    }
};

export class RemoveTaskStore extends CQL.RemoveOperation
{
    keyOverride:myTypes.DBentry;

    constructor(request:myTypes.InternalOperationDescription)
    {
        super(request);
        if(request.keyOverride === undefined) this.keyOverride = {};
        else this.keyOverride = request.keyOverride;
        this.buildQuery();
    }

    protected buildTableName():string
    {
        return "global_taskstore";
    }

    protected buildPrimaryKey():myTypes.Query
    {
        if(this.keyOverride === undefined) return {query:"", params:[]};
        let res = "";
        let params:string[] = [];
        Object.entries(this.keyOverride).forEach( ([key, value]) => 
        {
            res = res + key + " = ? AND ";
            params.push(value);
        });
        res = res.slice(0,-5);
        return {query: res, params: params};
    }
}
