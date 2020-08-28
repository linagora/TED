import * as myTypes from "../../utils/myTypes";
import config from "../../../services/configuration/configuration";
import {
  CQLBaseOperation,
  CQLSaveOperation,
  CQLGetOperation,
  CQLRemoveOperation,
  CQLBatchOperation,
  CQLOperationArray,
  CQLOperation,
} from "../adapters/cql/CQLOperations";
import {
  SQLBaseOperation,
  SQLSaveOperation,
  SQLGetOperation,
  SQLRemoveOperation,
  SQLOperationArray,
  SQLBatchOperation,
} from "../adapters/sql/SQLOperations";
import { createTable as cqlCreateTable } from "./../adapters/cql/TableCreation";
import { createTable as sqlCreateTable } from "./../adapters/sql/TableCreation";

export const tableCreationError: Error = new Error(
  "Table creation needed, canceling operation"
);

export let createTable: (def: myTypes.TableDefinition) => Promise<void>;

let core: "SQL" | "CQL" | undefined;
if (
  ["cassandra", "scylladb", "keyspace"].includes(
    config.configuration.ted.dbCore
  )
) {
  core = "CQL";
  createTable = cqlCreateTable;
} else if (["mongodb"].includes(config.configuration.ted.dbCore)) {
  core = "SQL";
  createTable = sqlCreateTable;
} else throw new Error("Unknown database core");

export abstract class BaseOperation implements myTypes.GenericOperation {
  action: myTypes.action;
  collections: string[];
  documents: string[];
  table: string | null;
  opID: string;
  operation: CQLBaseOperation | SQLBaseOperation | null;

  canCreateTable: boolean;

  constructor(request: myTypes.InternalOperationDescription) {
    this.action = request.action;
    this.collections = request.collections;
    this.documents = request.documents;
    this.canCreateTable = false;
    this.opID = request.opID;
    this.operation = null;
    this.table = null;
  }

  protected abstract buildOperation(): void;

  public async execute(): Promise<myTypes.ServerAnswer> {
    if (this.operation === null)
      throw new Error("unable to execute CQL operation, query not built");
    return this.operation.execute();
  }

  protected buildEntry(): myTypes.DBentry {
    try {
      let entry: myTypes.DBentry = {};
      for (let i: number = 0; i < this.documents.length; i++) {
        entry[this.collections[i]] = this.documents[i];
      }
      return entry;
    } catch (err) {
      throw new Error(
        "Wrong collection/document arguments in operation :" + err
      );
    }
  }

  public buildTableName(): string {
    let res: string[] = [];
    for (let i: number = 0; i < this.collections.length; i++) {
      res.push(this.collections[i]);
      res.push("_");
    }
    res = res.slice(0, -1);
    return res.join("");
  }

  public abstract async createTable(): Promise<void>;
}

export abstract class SaveOperation extends BaseOperation {
  object: string;
  options?: myTypes.SaveOptions;

  constructor(request: myTypes.InternalOperationDescription) {
    super(request);
    if (this.documents.length !== this.collections.length)
      throw new Error("Invalid path length parity for a save operation");
    if (request.encObject === undefined)
      throw new Error("Missing field object for a save operation");
    this.action = myTypes.action.save;
    this.object = request.encObject;
    this.options = request.options as myTypes.SaveOptions;
  }

  protected buildOperation(): void {
    if (this.object === undefined) throw new Error("Operation entry undefined");
    if (this.table === null) throw new Error("Undefined table");
    let entry = this.buildEntry();
    if (core === "CQL") {
      this.operation = new CQLSaveOperation({
        action: this.action,
        keys: entry,
        table: this.table,
        options: this.options === undefined ? {} : this.options,
      });
    } else {
      this.operation = new SQLSaveOperation({
        action: this.action,
        keys: entry,
        table: this.table,
        options: this.options === undefined ? {} : this.options,
      });
    }
  }

  protected buildEntry(): myTypes.DBentry {
    let entry = super.buildEntry();
    entry["object"] = this.object;
    return entry;
  }
}

export abstract class GetOperation extends BaseOperation {
  options?: myTypes.GetOptions;
  pageToken?: string;

  constructor(request: myTypes.InternalOperationDescription) {
    super(request);
    this.action = myTypes.action.get;
    this.options = request.options as myTypes.GetOptions;
  }

  protected buildOperation(): void {
    if (this.table === null) throw new Error("Undefined table");
    let entry = this.buildEntry();
    if (core === "CQL") {
      this.operation = new CQLGetOperation({
        action: this.action,
        keys: entry,
        table: this.table,
        options: this.options === undefined ? {} : this.options,
      });
    } else {
      this.operation = new SQLGetOperation({
        action: this.action,
        keys: entry,
        table: this.table,
        options: this.options === undefined ? {} : this.options,
      });
    }
  }
}

export abstract class RemoveOperation extends BaseOperation {
  constructor(request: myTypes.InternalOperationDescription) {
    super(request);
    this.action = myTypes.action.remove;
  }

  protected buildOperation(): void {
    if (this.table === null) throw new Error("Undefined table");
    let entry = this.buildEntry();
    if (core === "CQL") {
      this.operation = new CQLRemoveOperation({
        action: this.action,
        keys: entry,
        table: this.table,
      });
    } else {
      this.operation = new SQLRemoveOperation({
        action: this.action,
        keys: entry,
        table: this.table,
      });
    }
  }
}

export class BatchOperation implements myTypes.GenericOperation {
  action: myTypes.action;
  operationsArray: BaseOperation[];
  isolation: boolean;
  operation:
    | CQLBatchOperation
    | CQLOperationArray
    | SQLBaseOperation
    | SQLOperationArray
    | null;

  constructor(batch: BaseOperation[], isolation: boolean) {
    this.action = myTypes.action.batch;
    this.operationsArray = batch;
    this.isolation = isolation;
    this.operation = null;
    for (let op of this.operationsArray) {
      if (
        op.action === myTypes.action.batch ||
        op.action === myTypes.action.get
      )
        throw new Error("Batch cannot contain batch or get operations");
    }
  }

  public async execute(): Promise<myTypes.ServerAnswer> {
    this.buildOperation();
    if(this.operation === null) throw new Error("Error in batch, operation not built");
    return this.operation.execute()
    .catch(async (err:myTypes.CQLResponseError) =>
    {
      if((err.code === 8704 
        && err.message.substr(0,18) === "unconfigured table") 
        || err.message.match(/^Collection ([a-zA-z_]*) does not exist./))
      {
        await this.createAllTables();
        throw tableCreationError;
      }
      throw err;
    });
  }

  public push(operation: BaseOperation) {
    if (
      operation.action === myTypes.action.batch ||
      operation.action === myTypes.action.get
    )
      throw new Error("Batch cannot contain batch or get operations");
    this.operationsArray.push(operation);
  }

  protected buildOperation(): void {
    if (core === "CQL") {
      let cqlOperationArray: CQLBaseOperation[] = [];
      for (let op of this.operationsArray) {
        if (op.operation === null)
          throw new Error("Batch error, a base operation is not built");
        cqlOperationArray.push(op.operation as CQLBaseOperation);
      }
      if (this.isolation)
        this.operation = new CQLBatchOperation(cqlOperationArray);
      else this.operation = new CQLOperationArray(cqlOperationArray);
    } else {
      let sqlOperationArray: SQLBaseOperation[] = [];
      for (let op of this.operationsArray) {
        if (op.operation === null)
          throw new Error("Batch error, a base operation is not built");
        sqlOperationArray.push(op.operation as SQLBaseOperation);
      }
      if (this.isolation)
        this.operation = new SQLBatchOperation(sqlOperationArray);
      else this.operation = new SQLOperationArray(sqlOperationArray);
    }
  }

  protected async createAllTables(): Promise<void> {
    let promises: Promise<void>[] = [];
    for (let op of this.operationsArray) {
      if (op.canCreateTable) promises.push(op.createTable());
    }
    await Promise.all(promises);
  }

  protected async createTable(errmsg: string): Promise<void> {
    let parse = errmsg.match(/[\.a-zA-Z0-9_]*$/);
    if (parse === null)
      throw new Error("Unable to parse table name in batch error");
    let tableName = parse[0];
    console.log(tableName);
    for (let op of this.operationsArray) {
      let tmp = op.buildTableName();
      if (tmp === tableName) {
        await op.createTable();
        return;
      }
    }
    throw new Error(
      "Unable to find which operation triggered the error inside the batch " +
        tableName
    );
  }
}
