import * as myTypes from "../../../utils/myTypes";
import * as mongoDBTools from "./MongoDBtools";
import mongo, { ClientSession } from "mongodb";
import { mongodb } from "../../../../../config/config";

export type SQLOperation = SQLBaseOperation | SQLBatchOperation | SQLOperationArray;

export abstract class SQLBaseOperation implements myTypes.DBDirectOperation
{
    action:myTypes.action;
    table:string;
    queryValue:myTypes.DBentry;
    collection:mongo.Collection | null;
    
    constructor(infos:myTypes.CQLOperationInfos)
    {
        this.action = infos.action;
        this.table = infos.table;
        this.queryValue = infos.keys;
        this.collection = null;
    }

    public abstract async execute(session?:mongo.ClientSession):Promise<myTypes.ServerAnswer>;

    protected async buildQuery():Promise<void>
    {
        this.collection = await new Promise((resolve, reject) =>
        {
            mongoDBTools.database.collection(this.table, {strict:true}, (err:any, res:any) =>
            {
                if(err !== null)
                {
                    reject(err);
                }
                else
                {
                    resolve(res);
                }
            });
        });
    }
}

export class SQLSaveOperation extends SQLBaseOperation
{
    options?:mongo.ReplaceOneOptions;
    updateValue:myTypes.DBentry;

    tableCreationFlag:boolean = false;

    constructor(infos:myTypes.CQLOperationInfos)
    {
        super(infos);
        this.options = infos.options as mongo.ReplaceOneOptions;
        this.updateValue = {...this.queryValue};
        console.log(this.updateValue);
    }

    protected async buildQuery():Promise<void>
    {
        await super.buildQuery();

        delete this.queryValue["object"];
        if(this.options === undefined)
            this.options = {};
        this.options = {...this.options, ...{upsert: true}};
    }

    public async execute(session?:mongo.ClientSession):Promise<myTypes.ServerAnswer>
    {
        console.log(this);

        await this.buildQuery();
        if(this.collection === null)
            throw new Error("Uninitialized mongoDB collection");
        
        let res:mongo.ReplaceWriteOpResult;
        if(session === undefined)
            res = await this.collection.replaceOne(this.queryValue, this.updateValue, this.options)
        else
            res = await this.collection.replaceOne(this.queryValue, this.updateValue, {...{session}, ...this.options});
                
        if(res.result.ok === 1)
            return {status: "Success"};
        else
            throw new Error("failed to run operation");
    }
}

export class SQLGetOperation extends SQLBaseOperation
{
    options:myTypes.GetOptions;
    mongoOptions?:mongo.FindOneOptions<myTypes.ServerSideObject>;

    constructor(infos:myTypes.CQLOperationInfos)
    {
        super(infos);
        this.options = infos.options as myTypes.GetOptions;
    }

    protected async buildQuery():Promise<void>
    {
        await super.buildQuery();

        this.mongoOptions = {};
        this.mongoOptions.limit = this.options.limit;
        this.mongoOptions.skip = this.options.pageToken === undefined ?undefined : parseInt(this.options.pageToken);
        this.mongoOptions.sort = this.getMongoSort();
        this.buildWhereStatement();
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        console.log(this);

        await this.buildQuery();
        if(this.collection === null)
            throw new Error("Uninitialized mongoDB collection");
        
        let cursor = this.collection.find(this.queryValue, this.mongoOptions);
        let res = await cursor.toArray();
        let offset:number = res.length;
        if(this.mongoOptions?.skip !== undefined)
            offset += this.mongoOptions.skip;
        let answer:myTypes.ServerAnswer = {
            status: "Success",
            queryResults:{
                resultCount: res.length,
                allResultsEnc: [],
                allResultsClear: [],
                pageToken: this.options.limit === res.length ? offset.toString() : undefined,
            }
        };
        for(let result of res)
        {
            Object.keys(result).includes("object") ? answer.queryResults?.allResultsEnc?.push(result) : answer.queryResults?.allResultsClear?.push(result);
        }
        console.log(answer);
        return answer;
    }

    protected getMongoSort():Array<[string, number]> | undefined
    {
        if(this.options.order === undefined)
            return undefined;
        let res:Array<[string, number]> = [];
        for(let keyOrder of [this.options.order])
        {
            let orderValue = keyOrder.order === "ASC" ? 1 : -1;
            res.push([keyOrder.key, orderValue]);
        }
        return res;
    }

    protected buildWhereStatement():void
    {
        if(this.options.where === undefined)
            return;
        let where:myTypes.WhereClause = this.options.where;
        switch(where.operator)
        {
            case myTypes.Operator.eq:
            {
                this.queryValue[where.key] = { $eq: where.value};
                break;
            }
            case myTypes.Operator.diff:
            {
                this.queryValue[where.key] = { $ne: where.value};
                break;
            }
            case myTypes.Operator.geq:
            {
                this.queryValue[where.key] = { $gte: where.value};
                break;
            }
            case myTypes.Operator.gt:
            {
                this.queryValue[where.key] = { $gt: where.value};
                break;
            }
            case myTypes.Operator.leq:
            {
                this.queryValue[where.key] = { $lte: where.value };
                break;
            }
            case myTypes.Operator.lt:
            {
                this.queryValue[where.key] = { $lt: where.value };
                break;
            }
            case myTypes.Operator.in:
            {
                this.queryValue[where.key] = { $in: where.value};
                break;
            }
            case myTypes.Operator.notin:
            {
                this.queryValue[where.key] = { $nin: where.value};
                break;
            }
            default:
            {
                throw new Error("Operator not supported in SQL query");
            }
        }
    }
}

export class SQLRemoveOperation extends SQLBaseOperation
{
    constructor(infos:myTypes.CQLOperationInfos)
    {
        super(infos);
    }

    public async execute(session?:ClientSession):Promise<myTypes.ServerAnswer>
    {
        console.log(this);

        await this.buildQuery();

        if(this.collection === null)
            throw new Error("Uninitialized mongoDB collection");

        await this.collection.deleteOne(this.queryValue, {session});
        return {status: "Success"};
    }
}

export class SQLBatchOperation implements myTypes.DBDirectOperation
{
    action:myTypes.action = myTypes.action.batch;
    operations:SQLBaseOperation[];
    
    constructor(batch:SQLBaseOperation[])
    {
        this.operations = batch;
        for(let op of this.operations)
        {
          if(op.action === myTypes.action.batch || op.action === myTypes.action.get)
            throw new Error("Batch cannot contain batch or get operations");
        }
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        await mongoDBTools.runIsolatedBatch(this.operations);
        return {status: "Success"};
    }

    public push(operation:SQLBaseOperation)
    {
      if(operation.action === myTypes.action.batch || operation.action === myTypes.action.get) throw new Error("Batch cannot contain batch or get operations");
      this.operations.push(operation);
    }
}

export class SQLOperationArray implements myTypes.DBDirectOperation
{
    action:myTypes.action = myTypes.action.array;
    operations:SQLBaseOperation[];
    
    constructor(batch:SQLBaseOperation[])
    {
        this.operations = batch;
        for(let op of this.operations)
        {
          if(op.action === myTypes.action.batch || op.action === myTypes.action.get)
            throw new Error("Batch cannot contain batch or get operations");
        }
    }

    public async execute():Promise<myTypes.ServerAnswer>
    {
        await Promise.all(this.operations.map((op:SQLBaseOperation) => { return op.execute()}));
        return {status: "Success"};
    }

    public push(operation:SQLBaseOperation)
    {
      if(operation.action === myTypes.action.batch || operation.action === myTypes.action.get) throw new Error("Batch cannot contain batch or get operations");
      this.operations.push(operation);
    }
}