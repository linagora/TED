import * as amqp from "amqplib";
import { SQS } from "aws-sdk";
import { sqs, rabbitmq } from "../../../config/config";
import { delay } from "../utils/divers";
import { TaskPrefetcher} from "../utils/taskPrefetcher";

type ExternalResolver = {
    res:() => void,
    rej:() => void
}

const interruptionError = new Error("Task interrupted before end");

export abstract class TaskBroker
{
    queueOptions?:Object;
    messageOptions?:Object;
    callback:(data:any)=>Promise<void>;

    running:boolean;
    interruptor?:ExternalResolver;

    constructor(callback:(path:string)=>Promise<void>, queueOptions?:Object, messageOptions?:Object)
    {
        this.queueOptions = queueOptions;
        this.messageOptions = messageOptions;
        this.callback = callback;
        this.running = false;
    }

    public abstract async pushTask(task:string, ID:string):Promise<void>;
    public abstract async runTasks():Promise<void>;
    public stops():void
    {
        this.running = false;
        this.interruptor?.res();
    }
}

export class RabbitMQBroker extends TaskBroker
{
    queueName:string;
    amqpURL:string;
    channel?:amqp.Channel;
    connectionHandling:boolean;
    rejectionTimeout:number;
    prefetechCount:number;
    
    delayerResolver?:ExternalResolver;
    delayerPromise?:Promise<unknown>;

    constructor(amqpURL:string, queueName:string, callback:(path:string)=>Promise<void>, queueOptions:Object, messageOptions:Object, rejectionTimeout:number, preftechCount:number)
    {
        super(callback, queueOptions, messageOptions);
        this.queueName = queueName;
        this.amqpURL = amqpURL;
        this.connectionHandling = false;
        this.rejectionTimeout = rejectionTimeout;
        this.prefetechCount = preftechCount;
    }

    public async pushTask(task:string, ID:string):Promise<void>
    {
        if(this.channel === undefined)
        {
            await this.createChannel();
        }
        if(this.channel === undefined) throw new Error("Missing channel");
        await this.channel.assertQueue(this.queueName, this.queueOptions);
        this.channel.sendToQueue(this.queueName, Buffer.from(task, "utf-8"), this.messageOptions);
    }

    public async runTasks():Promise<void>
    {
        try
        {
            this.running = true;
            let interruptor:ExternalResolver = {res: ()=>{}, rej: ()=>{}};
            let ineterruptPromise = new Promise((resolve, reject) =>
            {
                interruptor.res = resolve;
                interruptor.rej = reject;
            });
            this.interruptor = interruptor;


            if(this.channel === undefined)
            {
                await Promise.race([this.createChannel(), ineterruptPromise]);
                if(!this.running)
                    throw interruptionError;
            }
            if(this.channel === undefined) throw new Error("Missing channel");
            await Promise.race([this.channel.assertQueue(this.queueName, this.queueOptions), ineterruptPromise]);
            await Promise.race([this.channel.prefetch(this.prefetechCount), ineterruptPromise]);
            if(!this.running)
                throw interruptionError;
            console.log("Connection to RabbitMQ successful, waiting for tasks")
            this.channel.consume(this.queueName, async (msg) => 
            {
                if(msg === null) throw new Error("Received null from amqp");
                try{
                    console.log("New task : ",msg.content.toString("utf-8"));
                    await Promise.race([this.callback(msg.content.toString("utf-8")), ineterruptPromise]);
                    if(!this.running)
                        throw interruptionError;
                    console.log("End of task : ", msg.content.toString("utf-8"));
                    this.channel?.ack(msg);
                }
                catch(err){
                    console.error(err);
                    await delay(this.rejectionTimeout);
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

    private async createChannel():Promise<void>
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
        else
        {
            await this.delayerPromise;
        }
    }
}

export class SQSBroker extends TaskBroker
{
    queueURL?:string;
    sqs:SQS;
    fifo:boolean;

    taskTable:TaskPrefetcher;

    constructor(callback:(path:string)=>Promise<void>, queueOptions:SQS.CreateQueueRequest, messageOptions:Object, prefetechCount:number, fifo:boolean)
    {
        super(callback, queueOptions, messageOptions);
        this.sqs = new SQS({
            region: sqs.region,
            accessKeyId: sqs.accessID,
            secretAccessKey: sqs.accessKey
        });
        this.fifo = fifo;
        this.taskTable = new TaskPrefetcher(prefetechCount);
    }

    public async pushTask(task:string, ID?:string):Promise<void>
    {
        return new Promise(async (resolve, reject) =>
        {
            try
            {
                if(this.queueURL === undefined) await this.createQueue();
                this.sqs.sendMessage({
                    QueueUrl: this.queueURL as string,
                    MessageBody: task,
                    MessageGroupId: this.fifo ? "projection" : undefined,
                    MessageDeduplicationId: this.fifo ? ID : undefined
                }, function(err, data):void {
                    if(err) 
                    {
                        reject(err);
                        return;
                    }
                    console.log("pushed op sqs :", task);
                    resolve();
                    return;
                });
            }
            catch(err){
                reject(err);
                return;
            }
        });    
    }

    private async runTask(ineterruptPromise:Promise<unknown>):Promise<void>
    {
        return new Promise(async (resolve, reject) =>
        {
            try{
                if(this.queueURL === undefined) await this.createQueue();
                const that = this;
                this.sqs.receiveMessage({QueueUrl: this.queueURL as string}, async function(err, data)
                {
                    try
                    {
                        if(err) reject(err);
                        if(data.Messages === undefined) 
                        {
                            console.log("no message recieved");
                            await delay(1000);
                            resolve(); 
                            return;
                        }
                        let msg:SQS.Message = data.Messages[0];
                        console.log("New task : ", msg.Body);
                        if(msg.Body === undefined) throw new Error("Received empty message");
                        await Promise.race([that.callback(msg.Body), ineterruptPromise]);
                        if(!that.running)
                            throw interruptionError;
                        that.sqs.deleteMessage({
                            QueueUrl: that.queueURL as string,
                            ReceiptHandle: msg.ReceiptHandle as string
                        }, function(err, data)
                        {
                            if(err) throw err;
                            console.log("End of task : ", msg.Body);
                            resolve();
                        });
                    }
                    catch(err)
                    {
                        reject(err);
                    }
                })
            }
            catch(err){
                reject(err);
            }
        });
    }

    public async runTasks():Promise<void>
    {
        try
        {
            this.running = true;
            let interruptor:ExternalResolver = {res: ()=>{}, rej: ()=>{}};
            let interruptPromise = new Promise((resolve, reject) =>
            {
                interruptor.res = resolve;
                interruptor.rej = reject;
            });
            this.interruptor = interruptor;

            while(1){
                if(!this.running)
                    throw interruptionError;
                try{
                    await this.taskTable.pushTask(this.runTask(interruptPromise));
                }
                catch(err){
                    if(err === interruptionError)
                        throw err;
                    console.log("End of task with error");
                    console.error(err);
                    await delay(1000);
                }
            let lock:ExternalResolver = {res: ()=>{}, rej: ()=>{}};
                let delayerPromise = new Promise(function (resolve, reject):void {
                    lock.res = resolve;
                    lock.rej = reject;
                });
            }
        }
        catch(err)
        {
            console.log("interruption error");
            console.error(err);
            return;
        }
    }

    private async createQueue():Promise<void>
    {
        console.log("trying to create queue");
        return new Promise((resolve, reject) =>
        {
        try
        {
            if(this.queueURL === undefined)
            {
                const that = this;
                this.sqs.createQueue(this.queueOptions as SQS.CreateQueueRequest, function(err, data):void
                {
                    if(err) throw err;
                    if(data.QueueUrl === undefined) throw new Error("Undefined queue URL");
                    console.log(data.QueueUrl);
                    that.queueURL = data.QueueUrl;
                    resolve(); 
                });
            }
            else resolve();
        }
        catch(err) {
            reject(err);
        }
        });
    }
}