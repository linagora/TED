import TED from "..";

export type SchemaDescription =
{
    default:boolean;
    [key:string]:true | false | undefined;
}

export type Schema =
{
    fullsearchIndex:SchemaDescription;
    wsPrivateKeys:SchemaDescription;
    dbSearchIndex:SchemaDescription;
}

export type TEDSchema =
{
    fullsearchIndex:string[];
    wsPrivateKeys:string[];
    dbSearchIndex:string[];
}

export type SchemaMap =
{
    [collection:string]:Schema;
}

export default class SchemasTable
{
    schemas:SchemaMap = {};

    public add(path:string, schema:Schema):void
    {
        let collectionPath = TED.getCollectionPath(path);
        this.schemas[collectionPath] = schema;
    }

    public get(path:string, object?:Object):TEDSchema
    {
        let keys:string[];
        let collectionPath = TED.getCollectionPath(path);
        let schema = this.schemas[collectionPath];

        if(object !== undefined) keys = Object.keys(object);
        else
        {
            keys = []
            let set:Set<string> = new Set();
            Object.keys(schema).forEach( (key:string) => {
                if(key !== "default") set.add(key);
            });
            set.forEach( (value:string) => {keys.push(value)});
        }

        return {
            fullsearchIndex: SchemasTable.tedify(schema.fullsearchIndex, keys),
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

}