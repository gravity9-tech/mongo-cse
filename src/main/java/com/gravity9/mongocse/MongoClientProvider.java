package com.gravity9.mongocse;

import com.mongodb.client.MongoClient;

import java.io.Closeable;

public interface MongoClientProvider extends Closeable {
    MongoClient getClient();

}
