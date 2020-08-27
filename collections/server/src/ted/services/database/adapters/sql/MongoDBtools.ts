import * as mongo from "mongodb";
import * as myTypes from "../../../utils/myTypes";
import { v4 as uuidv4 } from "uuid";
import config from "../../../../services/configuration/configuration";
import { SQLBaseOperation } from "./SQLOperations";

let client: mongo.MongoClient;
export let database: mongo.Db;

export async function setup(): Promise<void> {
  switch (config.configuration.ted.dbCore) {
    case "mongodb": {
      client = new mongo.MongoClient(config.configuration.mongodb.url, {
        useUnifiedTopology: true,
        useNewUrlParser: true,
      });
      await client.connect();
      database = client.db("twake_collections");
    }
  }
}

export async function runIsolatedBatch(
  batch: SQLBaseOperation[]
): Promise<void> {
  const session = client.startSession();

  try {
    await session.withTransaction(async () => {
      await Promise.all(
        batch.map((op: SQLBaseOperation) => op.execute(session))
      );
    });
  } finally {
    session.endSession();
  }
}
