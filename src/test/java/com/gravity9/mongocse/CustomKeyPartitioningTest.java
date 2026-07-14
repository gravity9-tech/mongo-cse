package com.gravity9.mongocse;

import com.gravity9.mongocse.listener.TestChangeStreamListener;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for custom (non-ObjectId) partition key support.
 *
 * These tests verify that partitioning works correctly with:
 * - String business keys
 * - Integer business keys
 * - Same key always goes to same partition
 * - Documents without key field are filtered out
 * - DELETE operations with custom key (requires changeStreamPreAndPostImages)
 */
class CustomKeyPartitioningTest extends AbstractMongoDbBase {

    private static final String CUSTOM_KEY_COLLECTION = "customKeyTestCollection";
    private static final String STRING_KEY_NAME = "businessKey";
    private static final String INTEGER_KEY_NAME = "customerId";

    private MongoCollection<Document> collection;

    @BeforeEach
    public void setup() {
        super.setup();
        collection = CLIENT_PROVIDER.getClient()
                .getDatabase(getDatabaseName())
                .getCollection(CUSTOM_KEY_COLLECTION);
    }

    @AfterEach
    public void tearDown() {
        collection.drop();
        CLIENT_PROVIDER.getClient().getDatabase(getDatabaseName())
                .getCollection(getWorkerConfigCollectionName())
                .deleteMany(Filters.eq("collection", CUSTOM_KEY_COLLECTION));
        CLIENT_PROVIDER.getClient().getDatabase(getDatabaseName())
                .getCollection(getClusterConfigCollectionName())
                .deleteMany(Filters.eq("collection", CUSTOM_KEY_COLLECTION));
    }

    @Test
    void givenStringBusinessKey_shouldPartitionAndReceiveEvents() throws Exception {
        MongoConfig config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(CUSTOM_KEY_COLLECTION)
                .keyName(STRING_KEY_NAME)
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        // Insert documents with String business keys
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, "ORDER-001", "amount", 100)));
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, "ORDER-002", "amount", 200)));
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, "INVOICE-100", "amount", 500)));

        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 3);

        assertEquals(3, events.size());

        // Verify all events are INSERTs with String business keys
        for (ChangeStreamDocument<Document> event : events) {
            assertEquals(OperationType.INSERT, event.getOperationType());
            assertNotNull(event.getFullDocument());
            Object businessKey = event.getFullDocument().get(STRING_KEY_NAME);
            assertNotNull(businessKey);
            assertInstanceOf(String.class, businessKey);
        }

        manager.stop();
    }

    @Test
    void givenIntegerBusinessKey_shouldPartitionAndReceiveEvents() throws Exception {
        MongoConfig config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(CUSTOM_KEY_COLLECTION)
                .keyName(INTEGER_KEY_NAME)
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        // Insert documents with Integer business keys
        collection.insertOne(new Document(Map.of(INTEGER_KEY_NAME, 1001, "name", "Customer A")));
        collection.insertOne(new Document(Map.of(INTEGER_KEY_NAME, 2002, "name", "Customer B")));
        collection.insertOne(new Document(Map.of(INTEGER_KEY_NAME, 3003, "name", "Customer C")));

        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 3);

        assertEquals(3, events.size());

        // Verify all events are INSERTs with Integer business keys
        for (ChangeStreamDocument<Document> event : events) {
            assertEquals(OperationType.INSERT, event.getOperationType());
            assertNotNull(event.getFullDocument());
            Object customerId = event.getFullDocument().get(INTEGER_KEY_NAME);
            assertNotNull(customerId);
            assertInstanceOf(Integer.class, customerId);
        }

        manager.stop();
    }

    @Test
    void givenSameBusinessKey_shouldAlwaysGoToSamePartition() throws Exception {
        MongoConfig config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(CUSTOM_KEY_COLLECTION)
                .keyName(STRING_KEY_NAME)
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();

        MongoCseManager manager = new MongoCseManager(config);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        TestChangeStreamListener listener1 = new TestChangeStreamListener();
        TestChangeStreamListener listener2 = new TestChangeStreamListener();
        manager.registerListener(listener0, List.of(0));
        manager.registerListener(listener1, List.of(1));
        manager.registerListener(listener2, List.of(2));
        manager.start();

        Thread.sleep(1000);

        // Insert multiple documents with the SAME business key
        String sameKey = "SAME-KEY-123";
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, sameKey, "sequence", 1)));
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, sameKey, "sequence", 2)));
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, sameKey, "sequence", 3)));

        Thread.sleep(1000);

        // Count events per partition
        int events0 = listener0.getEvents().size();
        int events1 = listener1.getEvents().size();
        int events2 = listener2.getEvents().size();

        // All 3 events should go to exactly ONE partition (same key = same hash = same partition)
        int totalEvents = events0 + events1 + events2;
        assertEquals(3, totalEvents, "Should receive all 3 events");

        // Exactly one partition should have all 3 events, others should have 0
        boolean onePartitionHasAll = (events0 == 3 && events1 == 0 && events2 == 0) ||
                                     (events0 == 0 && events1 == 3 && events2 == 0) ||
                                     (events0 == 0 && events1 == 0 && events2 == 3);

        assertTrue(onePartitionHasAll,
                "All events with same key should go to same partition. Got: p0=" + events0 + ", p1=" + events1 + ", p2=" + events2);

        manager.stop();
    }

    @Test
    void givenDocumentWithoutKeyNameField_shouldBeIgnored() throws Exception {
        MongoConfig config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(CUSTOM_KEY_COLLECTION)
                .keyName(STRING_KEY_NAME)  // Expecting "businessKey" field
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        // Insert document WITH the key field
        collection.insertOne(new Document(Map.of(STRING_KEY_NAME, "HAS-KEY", "data", "with key")));

        // Insert documents WITHOUT the key field - these should be filtered out
        collection.insertOne(new Document(Map.of("otherField", "value1", "data", "no key 1")));
        collection.insertOne(new Document(Map.of("otherField", "value2", "data", "no key 2")));
        collection.insertOne(new Document(Map.of("differentField", "value3", "data", "no key 3")));

        Thread.sleep(1500);

        List<ChangeStreamDocument<Document>> events = listener.getEvents();

        // Only the document WITH the key field should be received
        assertEquals(1, events.size(), "Only documents with the partition key field should be received");
        assertEquals("HAS-KEY", events.get(0).getFullDocument().getString(STRING_KEY_NAME));

        manager.stop();
    }

    @Test
    void givenDeleteWithCustomKeyName_shouldBeDetectedWithPreAndPostImages() throws Exception {
        // First, create collection with changeStreamPreAndPostImages enabled
        MongoDatabase db = CLIENT_PROVIDER.getClient().getDatabase(getDatabaseName());
        String prePostCollection = CUSTOM_KEY_COLLECTION + "_prepost";

        db.createCollection(prePostCollection, new CreateCollectionOptions()
                .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));

        MongoCollection<Document> prePostColl = db.getCollection(prePostCollection);

        try {
            MongoConfig config = MongoConfig.builder()
                    .connectionUri(getConnectionUri())
                    .databaseName(getDatabaseName())
                    .collectionName(prePostCollection)
                    .keyName(STRING_KEY_NAME)
                    .workerConfigCollectionName(getWorkerConfigCollectionName())
                    .clusterConfigCollectionName(getClusterConfigCollectionName())
                    .numberOfPartitions(3)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
                    .build();

            MongoCseManager manager = new MongoCseManager(config);
            TestChangeStreamListener listener = new TestChangeStreamListener();
            manager.registerListenerToAllPartitions(listener);
            manager.start();

            Thread.sleep(1000);

            // Insert a document
            String keyValue = "TO-BE-DELETED";
            prePostColl.insertOne(new Document(Map.of(STRING_KEY_NAME, keyValue, "data", "test")));

            Thread.sleep(500);

            // Delete the document
            prePostColl.deleteOne(Filters.eq(STRING_KEY_NAME, keyValue));

            List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 2);

            assertEquals(2, events.size(), "Should receive INSERT and DELETE events");

            // Find the DELETE event
            ChangeStreamDocument<Document> deleteEvent = events.stream()
                    .filter(e -> e.getOperationType() == OperationType.DELETE)
                    .findFirst()
                    .orElse(null);

            assertNotNull(deleteEvent, "DELETE event should be received");

            // Verify fullDocumentBeforeChange contains the business key
            Document beforeDoc = deleteEvent.getFullDocumentBeforeChange();
            assertNotNull(beforeDoc, "fullDocumentBeforeChange should be available for DELETE");
            assertEquals(keyValue, beforeDoc.getString(STRING_KEY_NAME),
                    "fullDocumentBeforeChange should contain the business key");

            manager.stop();
        } finally {
            // Cleanup
            prePostColl.drop();
            db.getCollection(getWorkerConfigCollectionName())
                    .deleteMany(Filters.eq("collection", prePostCollection));
            db.getCollection(getClusterConfigCollectionName())
                    .deleteMany(Filters.eq("collection", prePostCollection));
        }
    }

    @Test
    void givenUpdateWithCustomKeyName_shouldReceiveEventWithBeforeAndAfter() throws Exception {
        // Create collection with changeStreamPreAndPostImages enabled
        MongoDatabase db = CLIENT_PROVIDER.getClient().getDatabase(getDatabaseName());
        String prePostCollection = CUSTOM_KEY_COLLECTION + "_update";

        db.createCollection(prePostCollection, new CreateCollectionOptions()
                .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));

        MongoCollection<Document> prePostColl = db.getCollection(prePostCollection);

        try {
            MongoConfig config = MongoConfig.builder()
                    .connectionUri(getConnectionUri())
                    .databaseName(getDatabaseName())
                    .collectionName(prePostCollection)
                    .keyName(STRING_KEY_NAME)
                    .workerConfigCollectionName(getWorkerConfigCollectionName())
                    .clusterConfigCollectionName(getClusterConfigCollectionName())
                    .numberOfPartitions(3)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
                    .build();

            MongoCseManager manager = new MongoCseManager(config);
            TestChangeStreamListener listener = new TestChangeStreamListener();
            manager.registerListenerToAllPartitions(listener);
            manager.start();

            Thread.sleep(1000);

            // Insert a document
            String keyValue = "UPDATE-TEST";
            prePostColl.insertOne(new Document(Map.of(STRING_KEY_NAME, keyValue, "amount", 100)));

            Thread.sleep(500);

            // Update the document
            prePostColl.updateOne(
                    Filters.eq(STRING_KEY_NAME, keyValue),
                    Updates.set("amount", 200)
            );

            List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 2);

            assertEquals(2, events.size(), "Should receive INSERT and UPDATE events");

            // Find the UPDATE event
            ChangeStreamDocument<Document> updateEvent = events.stream()
                    .filter(e -> e.getOperationType() == OperationType.UPDATE)
                    .findFirst()
                    .orElse(null);

            assertNotNull(updateEvent, "UPDATE event should be received");

            // Verify fullDocument (after)
            Document afterDoc = updateEvent.getFullDocument();
            assertNotNull(afterDoc);
            assertEquals(200, afterDoc.getInteger("amount"));

            // Verify fullDocumentBeforeChange (before)
            Document beforeDoc = updateEvent.getFullDocumentBeforeChange();
            assertNotNull(beforeDoc, "fullDocumentBeforeChange should be available for UPDATE");
            assertEquals(100, beforeDoc.getInteger("amount"));

            manager.stop();
        } finally {
            prePostColl.drop();
            db.getCollection(getWorkerConfigCollectionName())
                    .deleteMany(Filters.eq("collection", prePostCollection));
            db.getCollection(getClusterConfigCollectionName())
                    .deleteMany(Filters.eq("collection", prePostCollection));
        }
    }

    @Test
    void givenDifferentKeyTypes_eachShouldGoToCorrectPartition() throws Exception {
        MongoConfig config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(CUSTOM_KEY_COLLECTION)
                .keyName("mixedKey")
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();

        MongoCseManager manager = new MongoCseManager(config);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        TestChangeStreamListener listener1 = new TestChangeStreamListener();
        TestChangeStreamListener listener2 = new TestChangeStreamListener();
        manager.registerListener(listener0, List.of(0));
        manager.registerListener(listener1, List.of(1));
        manager.registerListener(listener2, List.of(2));
        manager.start();

        Thread.sleep(1000);

        // Insert documents with different key types
        collection.insertOne(new Document(Map.of("mixedKey", "string-value", "type", "String")));
        collection.insertOne(new Document(Map.of("mixedKey", 12345, "type", "Integer")));
        collection.insertOne(new Document(Map.of("mixedKey", 99.99, "type", "Double")));

        Thread.sleep(1000);

        int totalEvents = listener0.getEvents().size() + listener1.getEvents().size() + listener2.getEvents().size();

        assertEquals(3, totalEvents, "All documents with different key types should be received");

        manager.stop();
    }
}
