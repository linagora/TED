import ES from "@elastic/elasticsearch";
import { elasticSearch } from "../../../../../config/config";
import { FullsearchInterface } from "../../FullsearchInterface";
import { ServerSideObject, FullsearchSchema, InternalOperationDescription } from "../../../utils/myTypes";

export class ESinterface extends FullsearchInterface
{
    client:ES.Client;

    constructor()
    {
        super();
        this.client = new ES.Client({ node: elasticSearch.url });
    }

    public async connect():Promise<void>
    {
        console.log("ES connecting...");
        await this.client.cluster.putSettings({ 
            body: {
                "persistent":{
                    "action.auto_create_index": "false"
                }
            }
        });
        console.log("Connected");
    }

    public async index(source_object:ServerSideObject, schema:FullsearchSchema, path:string):Promise<void>
    {
        try
        {
            let object = this.buildObject(source_object, schema, path);
            console.log(object);
            let indexName = this.getIndexName(path);
            let ID = this.getObjectID(path);
            await this.client.index({
                id:ID,
                index:indexName,
                body:object,
            })
        }
        catch(err)
        {
            if(err.message.match(/^index_not_found_exception$/))
            {
                await this.createIndex(schema, path);
                return this.index(source_object, schema, path);
            }
            console.error(err);
        }
    }

    public async search(query:Object, path:string):Promise<any>
    {
        let indexName = this.getIndexName(path);
        let ans = await this.client.search({
            index: indexName,
            body:query,
        })
        console.log(ans.body.hits.hits);
        let res:ServerSideObject[] = [];
        for(let hit of ans.body.hits.hits)
        {
            let tmp:ServerSideObject = {}
            Object.entries(hit["_source"]).forEach(([key, value]) =>
            {
                tmp[key.split(".")[1]] = value as string;
            });
            res.push(tmp);
        }
        console.log(res);
        return res;
    }

    public async update(source_object:ServerSideObject, schema:FullsearchSchema, path:string):Promise<void>
    {
        try
        {
            let object = this.buildObject(source_object, schema, path);
            console.log(object);
            let indexName = this.getIndexName(path);
            let ID = this.getObjectID(path);
            await this.client.update({
                id: ID,
                index: indexName,
                body:{
                    "doc":object,
                }
            })
        }
        catch(err)
        {
            if(err.message.match(/^index_not_found_exception$/))
            {
                await this.createIndex(schema, path);
                return this.update(source_object, schema, path);
            }
            if(err.message.match(/^document_missing_exception$/))
            {
                return this.index(source_object, schema, path);
            }
            console.error(err);
        }
    }

    protected async createIndex(schema:FullsearchSchema, path:string):Promise<void>
    {
        try
        {
            console.log("Creating new index...");
            
            let indexName = this.getIndexName(path);
            let keys = Object.keys(this.getKeys(path));
            let body:any = {
                "mappings":{
                    "_source":{
                        "includes": ["key.*"],
                        "excludes": ["value.*"]
                    },
                    "properties":{}
                }
            };
            for(let pair of schema)
            {
                body["mappings"]["properties"]["value." + pair.key] = { "type": pair.type };
            }
            for(let key of keys)
            {
                body["mappings"]["properties"]["key." + key] = { "type": "wildcard" };
            }
            console.log(body);
            await this.client.indices.create({
                index: indexName,
                body: body,
            });
            console.log("Done");
        }
        catch(err)
        {
            console.error(err);
            console.log(err.meta.body);
            throw err;
        }
        
    }

    protected buildObject(object:ServerSideObject, schema:FullsearchSchema, path:string):ServerSideObject
    {
        let res:ServerSideObject = {};
        for(let pair of schema)
        {
            res["value."+pair.key] = object[pair.key];
        }
        let keys = this.getKeys(path);
        Object.entries(keys).forEach(([key ,value]) => 
        {
            res["key." + key] = value;
        });
        return res;
    }
}