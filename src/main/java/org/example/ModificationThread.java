package org.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Map;
import java.util.UUID;


public class ModificationThread implements Runnable {
    private final String uri;
    private final String database;
    private final String collection;

    public ModificationThread(String uri, String database, String collection) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
    }

    @Override
    public void run() {
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(this.database);
            MongoCollection<Document> collection = database.getCollection(this.collection);

            do {
                collection.insertOne(new Document( Map.of("name" , UUID.randomUUID().toString())));
                System.out.println("added new");
                Thread.sleep(200);
            } while (true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
