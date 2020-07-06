import {Tedis} from "tedis";
import * as myTypes from "./myTypes";
import { buildPath, processPath } from "../MacroRoutines/RequestHandling"

export const tedis = new Tedis({
    host: "127.0.0.1",
    port: 6379
});

export async function pushOperation(path:string):Promise<void>
{
    let processedPath = processPath(path);
    if(processedPath.collections.length === processedPath.documents.length)
    {
        await tedis.rpush("projection_tasks", buildPath(processedPath.collections, processedPath.documents.slice(0,-1)));
        return;
    } 
    if(processedPath.collections.length === processedPath.documents.length + 1)
    {
        await tedis.rpush("projection_tasks", buildPath(processedPath.collections, processedPath.documents));
        return;
    }
    throw new Error("Invalid path");
}

export async function popOperation():Promise<string | null>
{
    return tedis.lpop("projection_tasks");
}