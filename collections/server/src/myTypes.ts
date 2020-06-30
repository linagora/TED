import {QueryOptions as QO} from "cassandra-driver";
import { Server } from "http";

export type QueryOptions = QO;

export enum action 
{
  save = "save",
  get = "get",
  remove = "remove",
  configure = "configure",
  batch = "batch",
  log = "log",
};

export type KeyspaceReplicationOptions = {
  class:"SimpleStrategy" | "NetworkTopologyStrategy" | "OldNetworkTopologyStrategy";
  replication_factor?:number;
  datacentersRF?:JSON;
};

export type SaveOptions = {
  ttl?:number;
};

export type Filter = {
  //TODO
};

export type Order = {
  //TODO
};

export type ServerBaseRequest = {
  action: action;
  path:string;
  object?:ServerSideObject;
  options?:SaveOptions | any;
  filter?:Filter;
  order?:Order;
  limit?:number;
  pageToken?:string;
  operations?:ServerBaseRequest[];
};

export type InternalOperationDescription = {
  action:action;
  primaryKey:string[];
  values:any[];
  clearObject?:ServerSideObject;
  encObject?:string;
  operations?:InternalOperationDescription[];
  options?:any;
};

export type ServerAnswer = {
  status:string;
  queryResults?:QueryResult;
  error?:CQLResponseError|string;
};

export type CQLResponseError = {
  name:string;
  info:string;
  message:string;
  code:number;
  query:string;
};

export type Query = {
  query:string;
  params:string[];
};

export type QueryResult = {
  resultCount:number;
  allResultsClear?:ServerSideObject[];
  allResultsEnc?:EncObject[];
};

export type ServerSideObject = {
  [key:string]:string;
  content:string;
};

export type DBentry = {
  [key:string]:string; //key=collections, values = documents
  object:string; //stringfy from an EncObject or objectUUID
};

export type EncObject = {
  data:string;
  iv:string;
  auth:string;
}

export type OperationID = {
  counter:number;
  timestamp:number;
}


export type TableDefinition = {
  name:string;
  keys:string[];
  types:string[];
  primaryKey:string[];
};

export type TableOptions = {
  nameExtension?:string;
  secondaryTable:boolean;
  //TODO
}

export interface Operation 
{
  action:action;
  execute():Promise<ServerAnswer>;
}

export type Log = {
  action:action;
  uuid:string;
  object?:string; //encrypted object
}

export type LogEntry = {
  [key:string]:string;
  log:string //stringify from a Log
}