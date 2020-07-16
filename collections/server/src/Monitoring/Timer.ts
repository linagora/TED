import { writeFile, readFile } from "fs"
import { ServerBaseRequest } from "../BaseTools/myTypes";

type LogMap = {
    [key:string]:number[];
}

type TimeTracker = {
    [key:string]:number;
}

type TimeTrackerLog = {
    [key:string]:TimeTracker[];
}

export class TimerLogsMap
{
    logs:LogMap;

    constructor()
    {
        this.logs = {};
        readFile("src/Monitoring/timers.json", "utf8", (err, data) => 
        {
            if(err) return err;
            this.logs = JSON.parse(data);
        });
    }

    public addTimeLog(key:string, time:number):void
    {
        if(this.logs[key] === undefined)
        {
            this.logs[key] = [];
        }
        this.logs[key].push(time);
        writeFile("src/Monitoring/timer.json", JSON.stringify(this.logs), "utf8", ()=>{});
    }
}

export class Timer
{
    key:string;
    start:number;
    static logMap:TimerLogsMap;

    constructor(key:string)
    {
        this.key = key;
        this.start = new Date().getTime();
    }

    public stop():number
    {
        let delta:number = new Date().getTime() - this.start;
        Timer.logMap.addTimeLog(this.key, delta);
        return delta;
    }
}

export class RequestTrackerLog
{
    logs:TimeTrackerLog;

    constructor()
    {
        this.logs = {};
        readFile("src/Monitoring/request_tracker.json", "utf8", (err, data) => 
        {
            if(err) return err;
            this.logs = JSON.parse(data);
        });
    }

    public addTracker(tracker:RequestTracker):void
    {
        let key:string = tracker.operation.action;
        if(this.logs[key] === undefined)
        {
            this.logs[key] = [];
        }
        this.logs[key].push(tracker.logs);
        writeFile("src/Monitoring/request_tracker.json", JSON.stringify(this.logs), "utf8", ()=>{});
    }
}

export class RequestTracker
{
    logs:TimeTracker;
    operation:ServerBaseRequest;
    last:number;
    static logMap:RequestTrackerLog;

    constructor(operation:ServerBaseRequest)
    {
        this.logs = {};
        this.last = new Date().getTime();
        this.operation = operation;
    }

    public endStep(step:string):void
    {
        let tmp = new Date().getTime();
        this.logs[step] = tmp - this.last;
        this.last = tmp;
    }

    public log():void
    {
        RequestTracker.logMap.addTracker(this);
    }
}