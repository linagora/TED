import {QueryOptions as QO} from "cassandra-driver";
import { SaveOperation, BaseOperation } from "./BaseOperations";

export type QueryOptions = QO;

export enum action 
{
  save = "save",
  get = "get",
  remove = "remove",
  batch = "batch",
  array = "array",
};

export enum Operator
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

export type KeyspaceReplicationOptions = {
  class:"SimpleStrategy" | "NetworkTopologyStrategy" | "OldNetworkTopologyStrategy";
  replication_factor?:number;
  datacentersRF?:JSON;
};

export type SaveOptions = {
  ttl?:number;
};

export type GetOptions = {
  limit: number,
  order: Order[],
  page: unknown,
  where: WhereClause
}

export type Order = {
  key:string,
  order:"ASC" | "DESC"
};

export type ServerBaseRequest = {
  action: action;
  path:string;
  object?:ServerSideObject;
  options?:SaveOptions | GetOptions;
  order?:Order;
  limit?:number;
  pageToken?:string;
  operations?:ServerBaseRequest[];
};

export type InternalOperationDescription = {
  action:action;
  collections:string[];
  documents:string[];
  opID:string;
  clearObject?:ServerSideObject;
  encObject?:string;
  operations?:InternalOperationDescription[];
  options?:any;
  tableOptions?:TableOptions;
  secondaryInfos?:SecondaryInfos;
  keyOverride?:DBentry;
  where?:WhereClause;
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
  allResultsEnc?:DBentry[];
};

export type ServerSideObject = {
  [key:string]:string|ServerSideObject;
  //content:string;
};

export type DBentry = {
  [key:string]:string; //key=collections, values = documents
};

export type EncObject = {
  data:string;
  iv:string;
  auth?:string;
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
  tableName?:string;
  secondaryTable:boolean;
  //TODO
}

export interface DBDirectOperation 
{
  action:action;
  execute():Promise<ServerAnswer>;
}

export interface GenericOperation
{
  action:action;
  operation:DBDirectOperation|null;
  execute():Promise<ServerAnswer>;
}

export type Log = InternalOperationDescription;

export type LogEntry = {
  [key:string]:string;
  object:string //stringify from a Log
}

export type SecondaryInfos = {
  secondaryKey:string;
  operator:Operator;
  secondaryValue:any;
}

export type WhereClause = {
  operator:Operator;
  field:string;
  value:any;
}

export type CQLOperationInfos = {
  action:action,
  keys:DBentry,
  table:string,
  options?:SaveOptions | GetOptions
}