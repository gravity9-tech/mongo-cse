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

1. Create the MongoCDCManager
2. Create and register your listeners
3. Start listening to events

```java
// Create manager and configs
MongoCDCManager manager = new MongoCDCManager(URI, DB_NAME, COLL_NAME, 3);

// Create listener dedicated to partitions 0 and 1
MyChangeStreamListener myListener = new MyChangeStreamListener();
manager.registerListener(myListener, List.of(0, 1));

// Create a generic listener registered to all partitions
MetricsChangeStreamListener metricsListener = new MetricsChangeStreamListener();
manager.registerListenerToAllPartitions(metricsListener);

// Start listening to events
manager.start();
```

You can register as many listeners to each and every partition as you'd like.

## Cosiderations

### Configs

MCSE works on the basis of configs. When used, it will create 2 collections in your MongoDB database: `changeStreamWorkerConfig` and `changeStreamClusterConfig`. They are used for:

* Storing the worker's collection name, partition and resumeToken 
* Making sure the number of partitions is maintained. If you need to re-partition the change streams - they will need to start over from the beginning of the change stream. You can modify the configs manually in those collections but it is not advised.

### Distributed environments

As of version 1.0.0 it is not yet possible to spread partitions across multiple JVMs. However, such a feature is planned for future releases.

Version 1.0.0 enables you, however, to handle change streams from different collections on different JVMs.