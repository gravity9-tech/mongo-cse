[![Pipeline][GitHub Acitons badge]][GitHub Acitons link]

## What is MCSE (Mongo Change Stream Enhancer)?

This is a lightweight library that allows Java developers to scale their usage of [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/). 

Mongo Change Stream Enhancer divides Change Stream events into partitions and enables you to handle them in separate Threads, increasing throughput. It achieves that by creating a Change Stream per each partition (number is configurable) and handling each Change Stream in a dedicated Thread.

## How does it work?

Inspired by solutions found in Kafka, we MCSE divides arriving events based on the document's `_id` field. We convert this field to a number and divide it by number of partitions. The remainder of the division is then taken as the ID of the partition.
This allows us to direct all events connected to the same document to the same partition - thus **guaranteeing that events will be handled in order** (as in Kafka - within the same partition).

### What's the aggregation used to create the change stream?

It's a `$match` aggregation:

`{"$expr": {"$eq": [{"$mod": [{"$divide": [{"$toLong": {"$toDate": "$_id"}}, 1000]}, $NUMBER_OF_PARTITIONS]}, $PARTITION_ID]}}`
## How to use it?

In order to use this library you need to do 2 things:

1. Create MongoConfig
2. Create the MongoCDCManager
3. Create and register your listeners by implementing the `ChangeStreamListener` interface
4. Start the workers and begin listening to events

For this example, I've created 2 listeners that I want to use `MyChangeStreamListener` and `MetricsChangeStreamListener`:

```java
public class MyChangeStreamListener implements ChangeStreamListener {
	@Override
	public void handle(ChangeStreamDocument<Document> event) {
		log.info("I've received an event! {}", event);
	}
}
```

```java
public class MetricsChangeStreamListener implements ChangeStreamListener {
	@Override
	public void handle(ChangeStreamDocument<Document> event) {
		logEventMetrics(event);
	}
}
```

Now, to configure the manager and register my listeners. I want to register `myListener` only for partitions 0 and 1, but I want `metricsListener` to handle events from all partitions.

Note that both listeners will receive all events from partitions 0 and 1, while `metricsListener` will also receive all events from partition 2. You can register as many listeners to each and every partition as you'd like. Partitions don't have to have any listeners assigned to them but the events will still be consumed by the worker and the resumeToken will be updated for that partition.

```java
// Create MongoConfig
MongoConfig mongoConfig = MongoConfig.builder()
		.connectionUri("mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority")
		.databaseName("test-db")
		.collectionName("example")
		.numberOfPartitions(3)
		.workerConfigCollectionName("changeStreamWorkerConfig")
		.clusterConfigCollectionName("changeStreamClusterConfig")
		.fullDocument(FullDocument.UPDATE_LOOKUP)
		.fullDocumentBeforeChange(FullDocumentBeforeChange.DEFAULT)
		.maxAwaitTimeInMs(1000)
		.build();

// Create manager and configs
MongoCDCManager manager = new MongoCDCManager(URI, DB_NAME, COLL_NAME, 3);

// Create listener dedicated to partitions 0 and 1
MyChangeStreamListener myListener = new MyChangeStreamListener();
manager.registerListener(myListener, List.of(0, 1));

// Create a generic listener registered to all partitions
MetricsChangeStreamListener metricsListener = new MetricsChangeStreamListener();
manager.registerListenerToAllPartitions(metricsListener);
```

Finally, I want to start listening to events. This call will start the worker threads, read the resumeTokens from the configs (if available) and start consuming events. 

```java
// Start listening to events
manager.start();
```

### MongoConfig configuration

* `connectionUri` - MongoDB URI
* `databaseName` - name of the database
* `collectionName` - name of the collection on which change stream listener should be applied
* `numberOfPartitions` - how many partitions should be used (how many parallel listeners can be run)
* `workerConfigCollectionName` - by default set to `changeStreamWorkerConfig`. Collection name in which worker config is stored
* `clusterConfigCollectionName` - by default set to `changeStreamClusterConfig`. collection name in which cluster config is stored
* `fullDocument` - by default set to `FullDocument.UPDATE_LOOKUP` to return the latest version of the document.
* `fullDocumentBeforeChange` - by default set to `FullDocumentBeforeChange.OFF`. It is used to return version of the document before applying the change.
* `maxAwaitTimeMS` - by default set to 1000 ms. The maximum amount of time in milliseconds the server waits for new data changes to report to the change stream cursor before returning an empty batch.


For more info about `fullDocument`, `fullDocumentBeforeChange` and `maxAwaitTime` see https://www.mongodb.com/docs/manual/reference/method/db.collection.watch/

## Cosiderations

### Configs

MCSE works on the basis of configs. When used, it will create 2 collections in your MongoDB database: `changeStreamWorkerConfig` and `changeStreamClusterConfig`. Names of these collections can be changed (see [MongoConfig configuration](#mongoconfig-configuration)). They are used for:

* Storing the worker's collection name, partition and resumeToken 
* Making sure the number of partitions is maintained. If you need to re-partition the change streams - they will need to start over from the beginning of the change stream. You can modify the configs manually in those collections but it is not advised.

This means that the MongoDB user needs to be able to create collections (or write to those collections if you create them manually). 

### Distributed environments

As of version 1.0.0 it is not yet possible to spread partitions across multiple JVMs. However, such a feature is planned for future releases.

Version 1.0.0 enables you, however, to handle change streams from different collections on different JVMs as MongoCDCManager's created for different collections are completely independent.

### MongoDB versions

This library was tested with the following MongoDB versions but should be working with all higher ones too:

* 4.4

We will try and expand the list with tests on other versions in the future.

## Running tests locally

### Setting up local MongoDB replica set

In order for the replica set to properly advertise it's node's addresses you should add the following mapping in your /etc/hosts file:

```
127.0.0.1       mongo1
127.0.0.1       mongo2
127.0.0.1       mongo3
```

After that, run `sh startReplicaSetEnvironment.sh` and wait for the replica set to start.

### Running tests

To run tests, simply run `mvn test` 

[GitHub Acitons badge]: https://github.com/gravity9-tech/mongocdc/actions/workflows/maven.yml/badge.svg?branch=main

[GitHub Acitons link]: https://github.com/gravity9-tech/mongocdc/actions/workflows/maven.yml
