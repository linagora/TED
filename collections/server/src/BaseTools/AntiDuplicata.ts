import { table } from "console";

type StateTable = {
    [key:string]:Boolean
}

type PromiseTable = {
    [key:string]:Promise<unknown>
}

type ResolverTable = {
    [key:string]:ExternalResolver
}

type ExternalResolver = {
    res:() => void,
    rej:() => void
}

export class TaskTable
{
    taskState:StateTable;
    promiseTable:PromiseTable;
    resolver:ResolverTable

    constructor()
    {
        this.taskState = {};
        this.resolver = {};
        this.promiseTable = {};
    }

    public isDone(task:string):boolean
    {
        if(this.taskState[task] === undefined || this.taskState[task]) return false;
        return true;
    }

    public isRunning(task:string):boolean
    {
        if(this.taskState[task] === undefined || ! this.taskState[task]) return false;
        return true;
    }

    public pushTask(task:string):void
    {
        if(! ( this.isRunning(task) || this.isDone(task)))
        {
            this.taskState[task] = true;
            let promiseExt:ExternalResolver = {res: ()=>{}, rej: ()=>{}};

            let promise = new Promise(function (resolve, reject):void {
                promiseExt.res = resolve;
                promiseExt.rej = reject;
            })
            this.promiseTable[task] = promise;
            this.resolver[task] = promiseExt;
            return;
        }
        throw new Error("Task already running");
    }

    public async waitTask(task:string):Promise<void>
    {
        await this.promiseTable[task];
    }

    public endTask(task:string):void
    {
        this.taskState[task] = false;
        this.resolver[task].res();
    }
}