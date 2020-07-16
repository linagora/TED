import {saveObject, randomObject, createdObjects, getObject, writeFusion, writeReadDelRead } from "./TestsUnitaires";
import { writeFile, readFile } from "fs"
import deepEqual from "deep-equal";
import { v4 as uuidv4 } from "uuid";
import { client } from "./../BaseTools/DatastaxTools";
import * as crypto from "crypto";

async function delay(ms:number):Promise<void>
{
    return new Promise( resolve => setTimeout(resolve, ms) );
}


//writeReadDelRead();

//getObject("b/2704dcd3-1f82-43e3-9da5-f539fd2e2110/aa/f61ed9ab-3cbd-4c98-ba86-6d369d610094/aaa/5f6d16e8-59bd-4388-9005-bc39cd52ee3f").then( (res) => console.log(res));

async function writeAndRead():Promise<void>
{
    let saveOps:Promise<void>[] = [];
    for (let i:number = 0; i<20; i++)
    {
        saveOps.push(saveObject(randomObject()));
    }

    await Promise.all(saveOps);
    await delay(10000);

    writeFile("src/__tests__/DBState.json", JSON.stringify(createdObjects), "utf8", () => console.log("Done"));

    let bool:Boolean = true;
    await Promise.all(Object.entries(createdObjects).map( async ([key, value]) => 
    {
        try{
            let dbversion = await getObject(key);
            console.log(dbversion);
            bool = bool && deepEqual(dbversion, value);
        }
        catch(err)
        {
            bool = false;
            console.log("============error=========\n", key);
            console.error(err);
        }
        
    }));
    console.log("Success : ", bool);
}
writeAndRead();

async function testCassandra():Promise<void>
{
    for (let index = 0; index < 1000; index++) {
        let obj = randomObject();
        let id = uuidv4();
        await client.execute("INSERT INTO a (a, object) VALUES (?,?)", [id, JSON.stringify(obj.object)], {prepare: true});
        let res = await client.execute("SELECT * FROM a WHERE a = ?", [id], {prepare:true});
        console.log(res["rows"]);
    }
}
//testCassandra();