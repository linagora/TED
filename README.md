# TED - The Extraordinary Database

<p align="center">
  <img width="240" height="240" src="ted.png">
  <br/>
  <i>This is Ted.</i> <b>Hello Ted! 👋</b>
</p>
<br/>

TED is a replicated, encrypted, scalable and realtime collections based database used in Twake. It works on top of Cassandra, Elastic-Search and RabbitMQ.

Read more about Twake collaborative workspace by Linagora on [https://twake.app](https://twake.app)

## Get started

Will start a MongoDB + TabbitMQ Ted server. If you want to configure Ted with Cassandra/Keyspaces or SQS, go to the complete documentation.

#### 1. Go to the right directory

```
cd collections/server/
```

#### 2. Create your config

```
# nano src/config/config.json
{
  "mongodb": {
    "url": "mongodb://some-mongo:27017/ted"
  },
  "rabbitmq": {
    "url": "amqp://some-rabbitmq"
  },
  "ted": {
    "broker": "RabbitMQ",
    "dbCore": "mongodb"
  }
}
```

#### 3. Run Ted

```
docker network create ted-network
docker run -d --network ted-network -it --name ted-mongo mongo
docker run -d --network ted-network -it --name ted-rabbitmq rabbitmq:3
# docker build -t twaketech/ted . #If you want to rebuild the code
docker run --network ted-network -it -v "/$(pwd)/src/config/:/usr/src/app/src/config/" -p 7250:7250 -p 7251:7251 twaketech/ted --config ./src/config/config.json
```

Ted will run on `localhost:7250`.

#### 4. Test Ted using our demo framework

```
cd ../framework/typescript/
yarn install
yarn start:demo
```

Then open Postman or anything and start creating / getting objects ! For instance:

```
# PUT http://localhost:9000/api/collections/company/0e44c200-c3ff-4a75-952e-d7e6130a70ed
{
    "object": {
        "name": "ACME"
    }
}
```

## Scalability

Ted is scalable, if you want to use Ted in multiple nodes, don't forget to add Redis for websockets distribution. Here is what you should add to the configuration:

```
  "redis": {
    "host": "localhost",
    "port": 6379
  },
```

Ted will automatically connect to Redis and start using it to share work between nodes.

## License

TED is licensed under [Affero GPL v3](http://www.gnu.org/licenses/agpl-3.0.html)

TED logo was generated with https://getavataaars.com/ ❤️
