import TEDServer, { Credentials } from "./TED/TedServer";
import DB, { TedRequest, SaveRequest, GetRequest, RemoveRequest } from "./TED/DB";
import BeforeOperation, { BeforeProcess } from "./TED/BeforeOperation";
import AfterOperation, { AfterProcess, AfterTask } from "./TED/AfterOperation";
import Schemas from "./TED/Schemas";
import express from "express";
import { Schema } from "inspector";

export type HTTPSaveBody = 
{
  object:Object;
}

export type HTTPGetBody = 
{
  order?:Order;
  limit?:number;
  pageToken?:string;
  where?:WhereClause;
  advancedSearch?:JSON;
}
type Order = {
  key:string,
  order:"ASC" | "DESC"
}
type WhereClause = {
  operator:Operator;
  key:string;
  value:any;
}
enum Operator
{
  eq = "=",
  diff = "!=",
  gt = ">",
  geq = ">=",
  lt = '<',
  leq = '<=',
  in = "IN",
  notin = "NOT IN"
}
  
export default class TED {

  before:BeforeOperation;
  after:AfterOperation;
  schemas:Schemas;
  server:TEDServer;
  db:DB;

  constructor()
  {
    this.before = new BeforeOperation();
    this.after = new AfterOperation();
    this.schemas = new Schemas();
    this.server = new TEDServer(this.after);
    this.db = new DB(this.server);
  }

  public bind(app:express.Express, route:string):void
  {
    let that = this;
    app.route(route + "/*")
    .put(async function (req, res, next)
    {
      try
      {
        let path = req.path.replace("/api/collections/", "");
        let response = await that.save(path, req.body, req);
        res.send(response);
      }
      catch(err)
      {
        res.send("Internal error : " + err.message);
      }
    })
    .get(async function (req, res, next)
    {
      try
      {
        let path = req.path.replace("/api/collections/", "");
        let response = await that.get(path, req.body, req);
        res.send(response);
      }
      catch(err)
      {
        res.send("Internal error : " + err.message);
      }
    })
    .delete(async function(req, res, next)
    {
      try
      {
        let path = req.path.replace("/api/collections/", "");
        let response = await that.remove(path, req);
        res.send(response);
      }
      catch(err)
      {
        res.send("Internal error : " + err.message);
      }
    });
  }

  public async save(path:string, save:HTTPSaveBody, originalRequest?:any):Promise<any>
  {
    let collectionPath = TED.getCollectionPath(path);
    let after:boolean = this.after.saves[collectionPath] !== undefined;
    let tedRequest:SaveRequest = {
      path: path,
      body:{
        action: "save",
        object: save.object,
        schema: this.schemas.schemas[collectionPath] !== undefined ? this.schemas.get(collectionPath, save.object) : undefined
      },
      afterTask: after
    };
    console.log(path);
    tedRequest = await this.before.runSave(tedRequest, originalRequest);
    let response = await this.db.save(tedRequest);
    return response;
  }

  public async get(path:string, get:HTTPGetBody, originalRequest?:any):Promise<any>
  {
    let collectionPath = TED.getCollectionPath(path);
    let after:boolean = this.after.gets[collectionPath] !== undefined;
    let tedRequest:GetRequest = {
      path:path,
      body:{
        action:"get",
        order:get.order,
        limit:get.limit,
        pageToken:get.pageToken,
        where:get.where,
        advancedSearch:get.advancedSearch
      },
    };
    console.log(path);
    tedRequest = await this.before.runGet(tedRequest, originalRequest);
    let response = await this.db.get(tedRequest);
    if(after)
    {
      this.after.run({
        action:"get",
        path: path,
        object: response.queryResults
      })
    }
    return response;
  }

  public async remove(path:string, originalRequest:any):Promise<any>
  {
    let collectionPath = TED.getCollectionPath(path);
    let after:boolean = this.after.removes[collectionPath] !== undefined;
    let tedRequest:RemoveRequest = {
      path: path,
      body:{
        action:"remove",
        schema: this.schemas.schemas[collectionPath] !== undefined ? this.schemas.get(collectionPath) : undefined
      },
      afterTask: after
    };

    tedRequest = await this.before.runRemove(tedRequest, originalRequest);
    let response = await this.db.remove(tedRequest);
    return response;
  }

  public async afterTasks(prefetch:number):Promise<void>
  {
    this.server.runTasks(prefetch);
  }

  public static getCollectionPath(path:string):string
  {
    let elems = path.split("/");
    let res:string[] = [];
    for(let i:number = 0; i < elems.length; i+=2) res.push(elems[i]);
    return res.join("/");
  }
}

export class HttpError extends Error {
  status = 500;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}