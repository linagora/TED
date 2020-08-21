import TED from "..";

export type AfterProcess = (object:Object) => Promise<void>;

type AfterProcessMap = {
    [path:string]:AfterProcess;
}

export type AfterTask =
{
    action:"save" | "get" | "remove";
    path:string;
    object:Object;
}

export default class AfterOperation
{
    saves:AfterProcessMap = {};
    gets:AfterProcessMap = {};
    removes:AfterProcessMap = {};

    public async run(task:AfterTask):Promise<void>
    {
        let handler:AfterProcess;
        let collectionPath = TED.getCollectionPath(task.path);
        switch(task.action)
        {
            case "save":
            {
                handler = this.saves[collectionPath];
                break;
            }
            case "get":
            {
                handler = this.gets[collectionPath];
                break;
            }
            case "remove":
            {
                handler = this.removes[collectionPath];
                break;
            }
        }
        if(handler === undefined) return;
        return handler(task.object);
    }

    public save(path:string, callback:AfterProcess)
    {
        this.saves[path] = callback;
    }

    public get(path:string, callback:AfterProcess)
    {
        this.gets[path] = callback;
    }

    public remove(path:string, callback:AfterProcess)
    {
        this.removes[path] = callback;
    }
}