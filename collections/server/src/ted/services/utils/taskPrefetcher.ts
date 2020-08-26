import { v4 as uuidv4 } from "uuid";

type TaskMap =
{
    [uuid:string]:Promise<void>;
}


export class TaskPrefetcher
{
    tasks:TaskMap;
    taskCounter:number;
    maxConcurrentTasks:number;

    constructor(maxConcurrentTasks:number)
    {
        this.tasks = {};
        this.taskCounter = 0;
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    public async pushTask(task:Promise<void>):Promise<void>
    {
        let taskID:string = uuidv4();
        console.log("Task ", taskID, " added to prefetecher");
        this.taskCounter +=1;
        this.tasks[taskID] = task;
        task.then( () => 
        {
            console.log("Ended task : ", taskID);
            this.taskCounter -=1;
            delete this.tasks[taskID];
        });
        await this.gate();
    }

    protected async gate():Promise<void>
    {
        if(this.taskCounter < this.maxConcurrentTasks)
            return;
        await Promise.race(Object.values(this.tasks));
    }
}