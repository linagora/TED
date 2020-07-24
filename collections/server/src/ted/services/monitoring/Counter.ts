import { writeFile, readFile } from "fs";
import * as prometheus from "prom-client";

type LogMap = {
    [key:string]:number
}

type promMap = {
    [key:string]:prometheus.Counter<string>
}

export class CounterMap
{
    counters:LogMap;
    prom_counters:promMap;

    constructor()
    {
        this.counters = {};
        this.prom_counters = {}
        /* readFile("src/Monitoring/logs/counter.json", "utf8", (err, data) => 
        {
            if(err) return;
            this.counters = JSON.parse(data);
        }); */
    }

    public inc(key:string):number
    {
        if(this.counters[key] === undefined || this.prom_counters[key] === undefined)
        {
            this.counters[key] = 0;
            this.prom_counters[key] = new prometheus.Counter({
                name: "custom_counter_" + key,
                help: "a generic custom counter related to " + key
            })
        }
        this.counters[key]++;
        this.prom_counters[key].inc();
        //writeFile("src/Monitoring/logs/counter.json", JSON.stringify(this.counters), "utf8", ()=>{});
        return this.counters[key];
    }
}