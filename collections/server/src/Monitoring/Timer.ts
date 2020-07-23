import { writeFile, readFile } from "fs"
import { ServerBaseRequest } from "../BaseTools/myTypes";
import * as prometheus from "prom-client";

type LogMap = {
    [key:string]:number[];
}

type promHistMap = {
    [key:string]:prometheus.Histogram<string>;
}

type promSumMap = {
    [key:string]:prometheus.Summary<string>;
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
    prom_logs:promHistMap;

    constructor()
    {
        this.logs = {};
        this.prom_logs = {};
        readFile("src/Monitoring/logs/timers.json", "utf8", (err, data) => 
        {
            if(err) return err;
            this.logs = JSON.parse(data);
        });
    }

    public addTimeLog(key:string, time:number):void
    {
        if(this.logs[key] === undefined || this.prom_logs[key] === undefined)
        {
            this.logs[key] = [];
            this.prom_logs[key] = new prometheus.Histogram({
                name: "custom_histogram_" + key,
                help: "a custom timer histogram related to " + key
            });
        }
        this.logs[key].push(time);
        this.prom_logs[key].observe(time);
        writeFile("src/Monitoring/logs/timer.json", JSON.stringify(this.logs), "utf8", ()=>{});
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
    prom_logs:promSumMap;

    constructor()
    {
        this.logs = {};
        this.prom_logs = {};
        readFile("src/Monitoring/logs/request_tracker.json", "utf8", (err, data) => 
        {
            if(err) return err;
            this.logs = JSON.parse(data);
        });
    }

    public addTracker(tracker:RequestTracker):void
    {
        let label:string = tracker.label;
        if(this.logs[label] === undefined)
        {
            this.logs[label] = [];
        }
        Object.entries(tracker.logs).forEach(([key, value]) => 
        {
            if(this.prom_logs[key] === undefined)
            {
                this.prom_logs[key] = new prometheus.Summary({
                    name: "custom_summary_tracker" + key,
                    help: "a custom summary to record the time of " +  key,
                    labelNames: ["operation_description"]
                })
            }
            this.prom_logs[key].observe({operation_description: label}, value);
        })
        this.logs[label].push(tracker.logs);
        writeFile("src/Monitoring/logs/request_tracker.json", JSON.stringify(this.logs), "utf8", ()=>{});
    }
}

export class RequestTracker
{
    logs:TimeTracker;
    label:string;
    operation:ServerBaseRequest;
    last:number;
    static logMap:RequestTrackerLog;

    constructor(operation:ServerBaseRequest, label:string)
    {
        this.logs = {};
        this.last = new Date().getTime();
        this.operation = operation;
        this.label = label;
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

    public updateLabel(label:string):void
    {
        this.label += "_" + label;
    }
}