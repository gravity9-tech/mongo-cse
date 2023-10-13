package org.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


public class ChangeStreamListener implements Runnable {

    private static Logger log = LoggerFactory.getLogger(ChangeStreamListener.class);

    private final String uri;
    private final String database;
    private final String collection;

    private final int partitionNumbers;
    private final int partition;

    public ChangeStreamListener(String uri, String database, String collection, int partitionNumbers, int partition) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
        this.partitionNumbers = partitionNumbers;
        this.partition = partition;
    }

    @Override
    public void run() {
        ConnectionString connectionString = new ConnectionString(uri);
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                pojoCodecRegistry);
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .codecRegistry(codecRegistry)
                .build();

        try (MongoClient mongoClient = MongoClients.create(clientSettings)) {
            MongoDatabase database = mongoClient.getDatabase(this.database);
            MongoCollection<ChangeStreamConfig> changeStreamListenerConfig = database.getCollection("changeStreamListenerConfig", ChangeStreamConfig.class);
            ChangeStreamConfig changeStreamConfig = changeStreamListenerConfig.find(Filters.and(List.of(
                    Filters.eq("collection", collection),
                    Filters.eq("partition", partition)
            ))).first();

            if (changeStreamConfig == null) {
                changeStreamConfig = new ChangeStreamConfig();
                changeStreamConfig.setCollection(collection);
                changeStreamConfig.setPartition(partition);

                changeStreamListenerConfig.insertOne(changeStreamConfig);

                changeStreamConfig = changeStreamListenerConfig.find(Filters.and(List.of(
                        Filters.eq("collection", collection),
                        Filters.eq("partition", partition)
                ))).first();

            }


            MongoCollection<Document> collection = database.getCollection(this.collection);

            ChangeStreamIterable<Document> watch = collection.watch(List.of(
                    Aggregates.match(
                            expr(eq(mod(divide(toLong(toDate())), partitionNumbers), partition))
                    )
            ));

            if (changeStreamConfig.getResumeToken() != null) {
                watch.resumeAfter(new BsonDocument("_data", new BsonString(changeStreamConfig.getResumeToken())));
            }

            do {
                try {
                    ChangeStreamDocument<Document> document = watch.cursor().tryNext();
                    if (document != null) {
                        System.out.println("partition" + partition + " resumeToken" + document.getResumeToken());
                        changeStreamConfig.setResumeToken(document.getResumeToken().getString("_data").getValue());

                        changeStreamConfig = changeStreamListenerConfig.findOneAndReplace(Filters.eq("_id", changeStreamConfig.getId()), changeStreamConfig);
                    }
                } catch (Exception ex) {
                    log.error("Exception while processing change", ex);
                }

            } while (true);
        }
    }

    private Bson expr(Bson expr) {
        return new Document("$expr", expr);
    }

    private Bson eq(Bson expr, int value) {
        return new Document("$eq", List.of(expr, value));
    }

    private Bson mod(Bson expr, int value) {
        return new Document("$mod", List.of(expr, value));
    }

    private Bson divide(Bson expr) {
        return new Document("$divide", List.of(expr, 1000));
    }

    private Bson toLong(Bson expr) {
        return new Document("$toLong", expr);
    }

    private Bson toDate() {
        return new Document("$toDate", "$fullDocument._id");
    }

    private ListenerInstance register(MongoDatabase database) {
        String name = Thread.currentThread().getName();
        System.out.println(name);

        MongoCollection<Document> changeListenerInstances = database.getCollection("changeListenerInstances");

        Document changeListenerInstance = changeListenerInstances.find(Filters.eq("name", name)).first();

        if (changeListenerInstance == null) {
            changeListenerInstances.insertOne(new Document("name", name));
            changeListenerInstance = changeListenerInstances.find(Filters.eq("name", name)).first();
        }

        return new ListenerInstance(changeListenerInstance.getObjectId("_id"), changeListenerInstance.getString("name"));
    }
}
