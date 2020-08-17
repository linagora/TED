import TEDServer, { Credentials } from "./TED/TedServer";
import DB, { TedRequest, SaveRequest, GetRequest, RemoveRequest } from "./TED/DB";
import BeforeOperation, { BeforeProcess } from "./TED/BeforeOperation";
import AfterOperation, { AfterProcess } from "./TED/AfterOperation";
import express from "express";
import { cpuUsage } from "process";

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
  advancedSearch:JSON;
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
  server:TEDServer;
  db:DB;

  constructor()
  {
    this.before = new BeforeOperation();
    this.after = new AfterOperation();
    this.server = new TEDServer(this.after);
    this.db = new DB(this.server);
  }

  public bind(app:express.Express, route:string):void
  {
    let that = this;
    app.route("/api/collections/*")
    .put(async function (req, res, next)
    {
      let path = req.path.replace("/api/collections/", "");
      let collectionPath = TED.getCollectionPath(path);
      let save:HTTPSaveBody = req.body;
      let after:boolean = that.after.saves[collectionPath] !== undefined;
      let tedRequest:SaveRequest = {
        path: path,
        body:{
          action:"save",
          object:save.object
        },
        afterSave: after
      };
      console.log(path);
      tedRequest = await that.before.runSave(tedRequest, req);
      let response = await that.db.save(tedRequest);
      res.send(response);
    })
    .get(async function (req, res, next)
    {
      let path = req.path.replace("/api/collections/", "");
      let collectionPath = TED.getCollectionPath(path);
      let get:HTTPGetBody = req.body;
      let after:boolean = that.after.gets[collectionPath] !== undefined;
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
        afterGet: after
      };
      console.log(path);
      tedRequest = await that.before.runGet(tedRequest, req);
      let response = await that.db.get(tedRequest);
      res.send(response);
    })
    .delete(async function(req, res, next)
    {
      let path = req.path.replace("/api/collections/", "");
      let collectionPath = TED.getCollectionPath(path);
      let after:boolean = that.after.removes[collectionPath] !== undefined;
      let tedRequest:RemoveRequest = {
        path: path,
        body:{
          action:"remove",
        },
        afterRemove: after
      };

      tedRequest = await that.before.runRemove(tedRequest, req);
      let response = await that.db.remove(tedRequest);
      res.send(response);
    });
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