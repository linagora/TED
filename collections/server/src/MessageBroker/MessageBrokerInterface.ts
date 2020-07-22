import * as amqp from "amqplib";
import { connect } from "http2";

type ExternalResolver = {
    res:() => void,
    rej:() => void
}

export abstract class TaskBroker
{
    queueName:string;
    queueOptions?:Object;
    messageOptions?:Object;

    constructor(queueName:string, queueOptions?:Object, messageOptions?:Object)
    {
        this.queueName = queueName;
        this.queueOptions = queueOptions;
        this.messageOptions = messageOptions;
    }

    public abstract async pushTask(task:string):Promise<void>;
    public abstract async runTasks():Promise<void>;
}

export class RabbitMQBroker extends TaskBroker
{
    amqpURL:string;
    channel?:amqp.Channel;
    connectionHandling:boolean;
    callback:(path:string)=>Promise<void>;

    delayerResolver?:ExternalResolver;
    delayerPromise?:Promise<unknown>;

    constructor(amqpURL:string, queueName:string, callback:(path:string)=>Promise<void>, queueOptions:Object, messageOptions:Object)
    {
        super(queueName, queueOptions, messageOptions);
        this.amqpURL = amqpURL;
        this.callback = callback;
        this.connectionHandling = false;
    }

    public async pushTask(task:string):Promise<void>
    {
        if(this.channel === undefined)
        {
            if(!this.connectionHandling)
            {
                this.connectionHandling = true;
                let delayer:ExternalResolver = {res: ()=>{}, rej: ()=>{}};
                this.delayerPromise = new Promise(function (resolve, reject):void {
                    delayer.res = resolve;
                    delayer.rej = reject;
                });
                this.delayerResolver = delayer;
    
                let conn = await amqp.connect(this.amqpURL);
                this.channel = await conn.createChannel();
    
                this.delayerResolver.res();
            }
            if(this.connectionHandling)
            {
                await this.delayerPromise;
            }
        }
        if(this.channel === undefined) throw new Error("Missing channel");
        await this.channel.assertQueue(this.queueName, this.queueOptions);
        this.channel.sendToQueue(this.queueName, Buffer.from(task, "utf-8"), this.messageOptions);
    }

    public async runTasks():Promise<void>
    {
        try
        {
            if(this.channel === undefined)
            {
                let conn = await amqp.connect(this.amqpURL);
                this.channel = await conn.createChannel();
            }
            await this.channel.assertQueue(this.queueName, this.queueOptions);
            await this.channel.prefetch(1);
            console.log("Connection to RabbitMQ successful, waiting for tasks")
            this.channel.consume(this.queueName, async (msg) => 
            {
                if(msg === null) throw new Error("Received null from amqp");
                try{
                    console.log("New task : ",msg.content.toString("utf-8"));
                    await this.callback(msg.content.toString("utf-8"));
                    console.log("End of task : ", msg.content.toString("utf-8"));
                    this.channel?.ack(msg);
                }
                catch(err){
                    this.channel?.reject(msg);
                }
                
            }, {noAck: false}); 
        }
        catch(err)
        {
            console.error("Unable to consume task from rabbitMQ");
            throw err;
        }
       
    }
}