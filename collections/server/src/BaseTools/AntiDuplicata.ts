import { ted } from "./../Config/config";

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
    resolver:ResolverTable;
    taskCounter:number;
    lock:ExternalResolver[];
    

    constructor()
    {
        this.taskState = {};
        this.resolver = {};
        this.promiseTable = {};
        this.taskCounter = 0;
        this.lock = [];
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

    public async pushTask(task:string):Promise<void>
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

            console.log(this.taskCounter, " running table creations");
            await this.delayer();
            this.taskCounter++;
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
        this.taskCounter--;
        this.taskState[task] = false;
        this.resolver[task].res();
        this.lock.map((promise)=>promise.res());
    }

    public failTask(task:string):void
    {
        this.taskCounter--;
        this.taskState[task] = false;
        this.resolver[task].rej();
        this.lock.map((promise)=>promise.rej());
    }

    private async delayer():Promise<void>
    {
        if(this.taskCounter < ted.maxTableCreation) return;
        let promiseExt:ExternalResolver = {res: ()=>{}, rej: ()=>{}};
        let lockPromise = new Promise(function (resolve, reject):void
        {
            promiseExt.res = resolve;
            promiseExt.rej = reject;
        });
        this.lock.push(promiseExt);
        await lockPromise;
    }
}