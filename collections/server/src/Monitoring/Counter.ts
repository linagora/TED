import { writeFile, readFile } from "fs"

type LogMap = {
    [key:string]:number
}

export class CounterMap
{
    counters:LogMap;

    constructor()
    {
        this.counters = {};
        readFile("src/Monitoring/counter.json", "utf8", (err, data) => 
        {
            if(err) return;
            this.counters = JSON.parse(data);
        });
    }

    public inc(key:string):number
    {
        if(this.counters[key] === undefined)
        {
            this.counters[key] = 0;
        }
        this.counters[key]++;
        writeFile("src/Monitoring/counter.json", JSON.stringify(this.counters), "utf8", ()=>{});
        return this.counters[key];
    }
}