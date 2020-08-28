import { ServerSideObject, FullsearchSchema, ServerAnswer } from "../utils/myTypes";
import { ServerSessionOptions } from "http2";

export abstract class FullsearchInterface
{

    protected getIndexName(path:string):string
    {
        let list = path.split("/");
        let res:string[] = [];
        for(let i:number = 0; i < list.length; i+=2)
        {
            res.push(list[i]);
        }
        return res.join("_");
    }

    protected getKeys(path:string):ServerSideObject
    {
        let res:ServerSideObject = {}
        let list = path.split("/");
        for(let i:number = 0; i < list.length; i+=2)
        {
            res[list[i]] = list[i+1];
        }
        return res;
    }

    protected getObjectID(path:string):string
    {
        return path.split("/").slice(-1)[0];
    }

    public abstract async index(object:ServerSideObject, schema:FullsearchSchema, path:string):Promise<void>;

    public abstract async search(query:Object, path:string):Promise<ServerSideObject[]>;

    public abstract async update(obejct:ServerSideObject, schema:FullsearchSchema, path:string):Promise<void>;

    public abstract async delete(schema:FullsearchSchema, path:string):Promise<void>;

    public abstract async connect():Promise<void>;
}