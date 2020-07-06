import {Tedis} from "tedis";
import { buildPath, processPath } from "../MacroRoutines/RequestHandling"

export const tedis = new Tedis({
    host: "127.0.0.1",
    port: 6379
});

export async function pushPending(path:string):Promise<void>
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

export async function popPending():Promise<string | null>
{
    return tedis.lpop("projection_tasks");
}

export async function peekPending():Promise<string | undefined>
{
    return (await tedis.lrange("projection_tasks", 0, 1))[0];
}

export async function removePending(path:string):Promise<void>
{
    await tedis.lrem("projection_tasks", 1, path);
}