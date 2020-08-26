import * as fs from "fs";
import * as _ from "lodash";

class Configuration {
  setup(configurationFile: string) {
    const config = JSON.parse(fs.readFileSync(configurationFile, "utf8"));
    this.configuration = _.merge(this.configuration, config);
    console.log(this.configuration);
  }

  //Contain default configuration
  public configuration: any = {
    //=============== Sentry config ===============

    sentry: {
      sentry: false,
      DSN: "enter DSN here",
    },

    //=============== Cassandra config ===============

    cassandra: {
      core: "cassandra",
      keyspace: "cassandra_collections",
      contactPoint: ["127.0.0.1"],
      localDatacenter: "datacenter1",
      defaultCassandraKeyspaceOptions: {
        class: "SimpleStrategy",
        replication_factor: 3,
      },
      keyspaceID: "",
      keyspaceKey: "",
    },

    mongodb: {
      url: "",
    },

    //=============== Redis config ===============

    redisNamespace: "twake_collections",

    //=============== RabbitMQ config ===============

    rabbitmq: {
      taskBroker: {
        queueName: "projection_tasks",
        URL: "amqp://localhost",
        queueOptions: {
          durable: true,
        },
        messageOptions: {
          persistent: true,
        },
        prefetchCount: 1,
        rejectionTimeout: 10000,
      },
      afterTaskBroker: {
        queueName: "after_tasks",
        URL: "amqp://localhost",
        queueOptions: {
          durable: true,
        },
        messageOptions: {
          persistent: true,
        },
        prefetchCount: 10,
        rejectionTimeout: 1000,
      },
    },

    //=============== SQS config ===============

    sqs: {
      region: "eu-central-1",
      accessID: "",
      accessKey: "",
      taskBroker: {
        queueOptions: {
          QueueName: "projection_tasks.fifo",
          Attributes: {
            FifoQueue: "true",
            VisibilityTimeout: "60",
          },
        },
        messageOptions: {},
        prefetchCount: 1,
      },
      afetrTaskBroker: {
        queueOptions: {
          QueueName: "after_tasks",
          Attributes: {
            VisibilityTimeout: "60",
          },
        },
        messageOptions: {},
        prefetchCount: 10,
      },
    },

    //=============== Crypto setup ===============

    crypto: {
      algorithm: "aes-256-gcm",
      keyLen: 32,
      password: "0000",
      salt: "00000000000000000000000000000000",
    },

    //=============== TED config ===============

    ted: {
      password: "ceci est un mot de passe",
      taskStoreBatchSize: 1,
      defaultTaskStoreTTL: 3600,
      broker: "SQS",
      maxTableCreation: 2,
      enableMultiTableCreation: true,
      dbCore: "cassandra",
    },
  };
}

export default new Configuration();
