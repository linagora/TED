import * as amqp from "amqplib";
import { SQS } from "aws-sdk";
import { sqs } from "./../Config/config";
import { delay } from "./../BaseTools/divers";

type ExternalResolver = {
    res:() => void,
    rej:() => void
}

export abstract class TaskBroker
{
    queueOptions?:Object;
    messageOptions?:Object;
    callback:(path:string)=>Promise<void>;

    constructor(callback:(path:string)=>Promise<void>, queueOptions?:Object, messageOptions?:Object)
    {
        this.queueOptions = queueOptions;
        this.messageOptions = messageOptions;
        this.callback = callback;
    }

    public abstract async pushTask(task:string, ID:string):Promise<void>;
    public abstract async runTasks():Promise<void>;
}

export class RabbitMQBroker extends TaskBroker
{
    queueName:string;
    amqpURL:string;
    channel?:amqp.Channel;
    connectionHandling:boolean;
    
    delayerResolver?:ExternalResolver;
    delayerPromise?:Promise<unknown>;

    constructor(amqpURL:string, queueName:string, callback:(path:string)=>Promise<void>, queueOptions:Object, messageOptions:Object)
    {
        super(callback, queueOptions, messageOptions);
        this.queueName = queueName;
        this.amqpURL = amqpURL;
        this.connectionHandling = false;
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
            if(this.channel === undefined)
            {
                await this.createChannel();
            }
            if(this.channel === undefined) throw new Error("Missing channel");
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

    constructor(callback:(path:string)=>Promise<void>, queueOptions:SQS.CreateQueueRequest, messageOptions:Object)
    {
        super(callback, queueOptions, messageOptions);
        this.sqs = new SQS({
            region: sqs.region,
            accessKeyId: sqs.accessID,
            secretAccessKey: sqs.accessKey
        })
    }

    public async pushTask(task:string, ID:string):Promise<void>
    {
        return new Promise(async (resolve, reject) =>
        {
            try
            {
                if(this.queueURL === undefined) await this.createQueue();
                this.sqs.sendMessage({
                    QueueUrl: this.queueURL as string,
                    MessageBody: task,
                    MessageGroupId: "projection",
                    MessageDeduplicationId: ID
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

    private async runTask():Promise<void>
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
                            console.log("no message received");
                            await delay(1000);
                            resolve(); 
                            return;
                        }
                        let msg:SQS.Message = data.Messages[0];
                        console.log("New task : ", msg.Body);
                        if(msg.Body === undefined) throw new Error("Received empty message");
                        await that.callback(msg.Body);
                        that.sqs.deleteMessage({
                            QueueUrl: that.queueURL as string,
                            ReceiptHandle: msg.ReceiptHandle as string
                        }, function(err, data)
                        {
                            if(err) throw err;
                            console.log("End of task : ", msg.Body);
                            resolve();
                        });
                        return;
                    }
                    catch(err)
                    {
                        reject(err);
                    }
                })
            }
            catch(err){
                reject(err);
                return;
            }
        });
    }

    public async runTasks():Promise<void>
    {
        while(1){
            try{
                await this.runTask();
            }
            catch(err){
                console.log("End of task with error");
                console.error(err);
                await delay(1000);
            }
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