import TED, { StringIndexedObject } from "..";

export type SchemaDescription =
{
    default:boolean;
    [key:string]:true | false | undefined;
}

export type FullsearchSchema =
{
    defaultIndexation:boolean;
    defaultTyping:EStyping;
    keys:{ [key:string]:string };
}

export type Schema =
{
    fullsearchIndex:FullsearchSchema;
    wsPrivateKeys:SchemaDescription;
    dbSearchIndex:SchemaDescription;
}

export type TEDFullsearchSchema = {key:string, type:string}[];

export type TEDSchema =
{
    fullsearchIndex:TEDFullsearchSchema;
    wsPrivateKeys:string[];
    dbSearchIndex:string[];
}

export type SchemaMap =
{
    [collection:string]:Schema;
}

type EStyping =
{
    string:"wildcard"|"keyword"|"text",
    number:"float"|"double"|"integer"|"long"|"short",
    boolean:"boolean",
    bigint:"integer"|"long"|"short",
}
export const defaultTyping:EStyping = {
    string:"wildcard",
    number:"float",
    boolean:"boolean",
    bigint:"long",
}

export default class SchemasTable
{
    schemas:SchemaMap = {};

    public add(path:string, schema:Schema):void
    {
        this.schemas[path] = schema;
    }

    public get(path:string, object?:StringIndexedObject):TEDSchema
    {
        let keys:string[];
        let collectionPath = TED.getCollectionPath(path);
        let schema = this.schemas[collectionPath];

        if(object !== undefined) keys = Object.keys(object);
        else
        {
            keys = []
            let set:Set<string> = new Set();
            Object.values(schema).forEach( (schemaDescriptor) => 
            {
                Object.keys(schemaDescriptor).forEach( (key:string) => {
                    if(key !== "default") set.add(key);
                });
            });
            set.forEach( (value:string) => {keys.push(value)});
            console.log(keys);
        }

        return {
            fullsearchIndex: SchemasTable.tedifyFS(schema.fullsearchIndex, keys, object),
            wsPrivateKeys: SchemasTable.tedify(schema.wsPrivateKeys, keys),
            dbSearchIndex: SchemasTable.tedify(schema.dbSearchIndex, keys)
        };
    }

    private static tedify(schema:SchemaDescription, keys:string[]):string[]
    {
        let res:string[] = [];
        for(let key of keys)
        {
            if( schema[key] === true || (schema[key] === undefined && schema.default))
                res.push(key);
        }
        return res;
    }

    private static tedifyFS(schema:FullsearchSchema, keys:string[], object?:StringIndexedObject):TEDFullsearchSchema
    {
        let res:TEDFullsearchSchema = [];
        for(let key of keys)
        {
            if( schema.keys[key] !== undefined)
            {
                res.push({key: key, type: schema.keys[key]});
            }
            else if(object !== undefined && schema.defaultIndexation && Object.keys(object).includes(key))
            {
                switch(typeof(object[key]))
                {
                    case "function":
                    case "object":
                    case "undefined":
                    case "symbol":
                    {
                        throw new Error("Types unsupported for fullsearch yet");
                    }
                    case "number":
                    {
                        res.push({key: key, type: schema.defaultTyping.number});
                        break;
                    }
                    case "string":
                    { 
                        res.push({key: key, type: schema.defaultTyping.string});
                        break;
                    }
                    case "boolean":
                    {
                        res.push({key: key, type: schema.defaultTyping.boolean});
                        break;
                    }
                    case "bigint":
                    {
                        res.push({key: key, type: schema.defaultTyping.bigint});
                        break;
                    }
                }
            }
        }
        return res;
    }
}