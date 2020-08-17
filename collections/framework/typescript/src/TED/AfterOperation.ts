import TED from "..";
import { SaveRequest, GetRequest, RemoveRequest } from "./DB";

export type AfterProcess = (object:Object) => Object;

type AfterProcessMap = {
    [path:string]:AfterProcess;
}  

export default class AfterOperation
{
    saves:AfterProcessMap = {};
    gets:AfterProcessMap = {};
    removes:AfterProcessMap = {};

    public async runSave(data:SaveRequest):Promise<any>
    {
        let collectionPath = TED.getCollectionPath(data.path);
        if(this.saves[collectionPath] !== undefined)
            return this.saves[collectionPath](data);
        return data;
    }

    public async runGet(data:GetRequest):Promise<any>
    {
        let collectionPath = TED.getCollectionPath(data.path);
        if(this.gets[collectionPath] !== undefined)
            return this.gets[collectionPath](data);
        return data;
    }

    public async runRemove(data:RemoveRequest):Promise<any>
    {
        let collectionPath = TED.getCollectionPath(data.path);
        if(this.removes[collectionPath] !== undefined)
            return this.saves[collectionPath](data);
        return data;
    }

    public save(path:string, callback:AfterProcess)
    {
        let collectionPath = TED.getCollectionPath(path);
        this.saves[collectionPath] = callback;
    }

    public get(path:string, callback:AfterProcess)
    {
        let collectionPath = TED.getCollectionPath(path);
        this.gets[collectionPath] = callback;
    }

    public remove(path:string, callback:AfterProcess)
    {
        let collectionPath = TED.getCollectionPath(path);
        this.removes[collectionPath] = callback;
    }
}