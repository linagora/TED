export async function delay(ms:number):Promise<void>
{
    return new Promise( resolve => setTimeout(resolve, ms) );
}

export function processPath(path:string):{documents:string[], collections:string[]}
{
    if(path === undefined) return {documents:[], collections:[]};
    path = path.toLowerCase();
    let documents:string[] = [];
    let collections:string[] = [];
    let names:string[] = path.split('/');
    for(let i:number = 0; i<names.length; i++)
    {
        if(i%2 === 0)
        {
            collections.push(names[i]);
            let nameCtrl = names[i].match("^[a-z]*$");
            if(nameCtrl === null) throw new Error("Invalid collection name");
        } 
        else
        {
            documents.push(names[i]);
            let nameCtrl = names[i].match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
            if(nameCtrl === null) throw new Error("Invalid document ID");
        }      
    }
    return {documents:documents, collections:collections};
}

export function buildPath(collections:string[], documents:string[], truncate:boolean):string
{
    if(collections.length - documents.length > 1 || collections.length - documents.length < 0) throw new Error("Invalid documents[] and collections[] length");
    let res:string[] = [];
    for(let i:number = 0; i < documents.length; i++)
    {
        res.push(collections[i]);
        res.push(documents[i]);
    }
    if(collections.length - documents.length === 1) res.push(collections.slice(-1)[0]);
    else if(truncate) res.pop();
    return res.join("/");
}

export function truncatePath(path:string):string
{
    let list:string[] = path.split("/");
    if(list.length % 2 === 0 ) list = list.slice(0,-1);
    return list.join("/");
}