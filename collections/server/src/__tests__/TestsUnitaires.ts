import { v4 as uuidv4 } from "uuid";
import post from 'axios';
import { writeFile, readFile } from "fs";


function randint(min:number, max:number) { // min and max included 
    return Math.floor(Math.random() * (max - min + 1) + min);
};

const mminDepth = 1;
const maxDepth = 1;

type map = {
    [key:string]:Object
}
export let createdObjects:map = {};

readFile('src/__tests__/DBState.json', 'utf8', function readFileCallback(err, data){
    if (err){
        console.log(err);
    } else {
    createdObjects = JSON.parse(data); //now it an object
}});

let names:string[] = ["a", "b"];

type DBObject = {
    path:string;
    object: {
        [key: string]: any
    };
};

export function randomObject():DBObject
{
    let depth = randint(mminDepth, maxDepth);
    let pathElem:string[] = [];
    for(let i:number = 0; i<depth ; i++)
    {
        pathElem.push(names[randint(0,names.length -1)].repeat(i+1));
        pathElem.push(uuidv4());
    }
    let path:string = pathElem.join("/");
    let param:number = randint(1,3);
    let obj: DBObject = {path: path, object: {}};
    for (let i: number = 0; i<param; i++){
        obj.object["test"+i] = randint(0, 10);
    }
    return obj;
}

export async function saveObject(obj:DBObject):Promise<void>
{
    const response = await post("http://localhost:8080/", {
        data: {
            action: "save",
            path: obj.path,
            object: obj.object
        }
    });
    console.log(obj);
    createdObjects[obj.path] = obj.object;
    console.log(response.data);
}

export async function getObject(path:string):Promise<Object>
{
    const response = await post("http://localhost:8080", {
        data: {
            action: "get",
            path: path
        }
    })
    if(response.data.queryResults === undefined) 
    {
        console.log(response.data);
        throw new Error("Response error\n");
    }
    if(response.data.queryResults.allResultsClear === undefined) 
    {
        console.log(response.data);
        throw new Error("Missing results\n");
    }    
    if(response.data.queryResults.allResultsClear[0] === undefined) 
    {
        console.log(response.data);
        throw new Error("Missing results\n");
    }
    return response.data.queryResults.allResultsClear[0].object;
}

export async function getObjectWhere(path:string, key:string, value:number):Promise<Object[]>
{
    const response = await post("http://localhost:8080", {
        data: {
            action: "get",
            path: path,
            where:{
                operator:"=",
                field:key,
                value:value
            }
        }
    })
    if(response.data.queryResults === undefined) 
    {
        console.log(response.data);
        throw new Error("Response error\n");
    }
    if(response.data.queryResults.allResultsClear === undefined) 
    {
        console.log(response.data);
        throw new Error("Missing results\n");
    }    
    return response.data.queryResults.allResultsClear;
}

export async function writeFusion():Promise<void>
{
  let path = randomObject().path;
  let promises:Promise<void>[] = [];
  for (let index = 0; index < 3; index++) {
    let key:string = "key"+index;
    let obj:map = {};
    obj[key] = index;
    promises.push(saveObject({path:path, object:obj})); 
  }
  await Promise.all(promises);
  console.log(await getObject(path));
}

export async function removeObject(path:string):Promise<void>
{
    const response = await post("http://localhost:8080/", {
        data: {
            action: "remove",
            path: path
        }
    });
    console.log(response.data);
}

export async function writeReadDelRead():Promise<void>
{
    let obj = randomObject();
    let path = obj.path;
    console.log(obj);
    await saveObject(obj);
    await delay(1000);
    console.log(await getObject(path));
    await removeObject(path);
    console.log(await getObject(path));
}

async function delay(ms:number):Promise<void>
{
    return new Promise( resolve => setTimeout(resolve, ms) );
}