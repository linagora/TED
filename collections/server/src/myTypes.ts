import {QueryOptions as QO} from "cassandra-driver";

export type QueryOptions = QO;

export enum action 
{
  save = "save",
  get = "get",
  remove = "remove",
  configure = "configure",
  batch = "batch"
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
  object?:ClearObject | EncObject;
  options?:SaveOptions | any;
  filter?:Filter;
  order?:Order;
  limit?:number;
  pageToken?:string;
  operations?:ServerBaseRequest[];
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
  allResults:ClearObject[] | EncObject[];
};

export type ClearObject = {
    [primaryKey:string]:string;
    content:string; //clear content
}

export type EncObject = {
  [key:string]:string;
  content:string; //stringfy from a DBentry
};

export type DBentry = {
    content:string;
    iv?:string;
    auth?:string;
}



export type TableOptions = {
 //TODO
};

export interface Operation 
{
  action:action;
  execute():Promise<ServerAnswer>;
}