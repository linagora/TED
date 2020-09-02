import { ServerSideObject } from "./myTypes";

type reverseMap = 
{
    [uuid:string]:number;
}

export class Orderer
{
    revMap:reverseMap;

    constructor(uuids:string[])
    {
        this.revMap = Orderer.buildRevMap(uuids);
    }

    private static buildRevMap(uuids:string[]):reverseMap
    {
        let res:reverseMap = {};
        for(let i = 0 ; i<uuids.length ; i++)
        {
            res[uuids[i]] = i;
        }
        return res;
    }

    public order(objects:ServerSideObject[], key:string):ServerSideObject[]
    {
        let res:ServerSideObject[] = new Array<ServerSideObject>(this.revMap.length);
        for(let val of objects)
        {
            let uuid:string = val[key] as string;
            let idx:number = this.revMap[uuid];
            res[idx] = val;
        }
        return res;
    }
}