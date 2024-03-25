package com.gravity9.mongocse;

import com.gravity9.mongocse.listener.TestChangeStreamListener;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MongoDBContainer;

import java.util.List;

public abstract class AbstractMongoDbBase {

    private static final MongoDBContainer MONGO_DB_CONTAINER =
            new MongoDBContainer("mongo:4.2.8");
    protected static MongoClientProvider CLIENT_PROVIDER;
    private static final String COLL_NAME = "testCollection";
    private static final String DB_NAME = "test";

    private MongoDatabase mongoDatabase;

    protected String getConnectionUri() {
        return MONGO_DB_CONTAINER.getReplicaSetUrl();
    }

    protected String getTestCollectionName() {
        return COLL_NAME;
    }

    protected String getDatabaseName() {
        return DB_NAME;
    }

    protected MongoClientProvider getClientProvider() {
        return CLIENT_PROVIDER;
    }

    protected String getWorkerConfigCollectionName() {
        return "changeStreamWorkerConfig";
    }

    protected String getClusterConfigCollectionName() {
        return "changeStreamClusterConfig";
    }

    @BeforeAll
    public static void setUpAll() {
        MONGO_DB_CONTAINER.start();
        CLIENT_PROVIDER = new MongoClientProvider(MONGO_DB_CONTAINER.getReplicaSetUrl());
    }

    @AfterAll
    public static void tearDownAll() {
        if (!MONGO_DB_CONTAINER.isShouldBeReused()) {
            MONGO_DB_CONTAINER.stop();
            CLIENT_PROVIDER.close();
        }
    }

    @BeforeEach
    public void setup() {
        mongoDatabase = CLIENT_PROVIDER.getClient().getDatabase(getDatabaseName());
    }

    @AfterEach
    public void cleanUp() {
        mongoDatabase.getCollection(getTestCollectionName()).drop();
        mongoDatabase.getCollection(getWorkerConfigCollectionName())
                .deleteMany(Filters.eq("collection", getTestCollectionName()));
        mongoDatabase.getCollection(getClusterConfigCollectionName())
                .deleteMany(Filters.eq("collection", getTestCollectionName()));
    }

    protected static List<ChangeStreamDocument<Document>> waitForEvents(TestChangeStreamListener listener, int expectedCount) {
        List<ChangeStreamDocument<Document>> result;
        int testNo = 1;

        do {
            // Wait for CDC event to arrive
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            result = listener.getEvents();
            testNo++;
        } while (
                testNo < 10
                        && (
                        (expectedCount == 0 && result.isEmpty())
                                || (expectedCount > 0 && result.size() < expectedCount)
                )
        );

        return result;
    }


}
