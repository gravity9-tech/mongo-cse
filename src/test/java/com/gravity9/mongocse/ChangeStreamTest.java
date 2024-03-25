package com.gravity9.mongocse;

import com.gravity9.mongocse.constants.TestIds;
import com.gravity9.mongocse.listener.TestChangeStreamListener;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ChangeStreamTest extends AbstractMongoDbBase {

    private static final Logger log = LoggerFactory.getLogger(ChangeStreamTest.class);

    private MongoCollection<Document> collection;

    private MongoConfig mongoConfig;

    @BeforeEach
    public void setup() {
        super.setup();
        mongoConfig = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();
        collection = CLIENT_PROVIDER.getClient().getDatabase(getDatabaseName()).getCollection(getTestCollectionName());
    }

    @AfterEach
    public void tearDown() {
        super.cleanUp();
    }

    @Test
    void givenProperConfiguration_shouldStartManagerAndRegisterListener() throws Exception {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        // Wait for manager to start
        Thread.sleep(1000);

        Document testDoc = new Document("testValue", 123);
        InsertOneResult insertOneResult = collection.insertOne(testDoc);
        log.info("Inserted document: {}", insertOneResult.getInsertedId());

        int expectedEventCount = 1;
        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, expectedEventCount);

        assertEquals(expectedEventCount, events.size());
        assertEquals(OperationType.INSERT, events.get(0).getOperationType());
        assertEquals(123, events.get(0).getFullDocument().getInteger("testValue"));
    }

    @Test
    void listenerReceivesAllBasicOpTypes() throws Exception {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        // Wait for manager to start
        Thread.sleep(1000);

        final var initialValue = 123;
        final var newValue = 99;

        Document testDoc = new Document("testValue", initialValue);
        InsertOneResult insertOneResult = collection.insertOne(testDoc);
        ObjectId insertedId = insertOneResult.getInsertedId().asObjectId().getValue();
        log.info("Inserted document: {}", insertedId.toHexString());

        Bson updates = Updates.set("testValue", newValue);

        Document query = new Document().append("testValue", initialValue);

        UpdateResult updateResult = collection.updateOne(query, updates);

        var deleteResult = collection.deleteOne(Filters.eq("_id", insertedId));

        int expectedEventCount = 3;
        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, expectedEventCount);

        assertEquals(1, updateResult.getModifiedCount());
        assertEquals(1, deleteResult.getDeletedCount());

        assertEquals(expectedEventCount, events.size());
        assertEventsContainNumberOfOpTypes(events, OperationType.INSERT, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.DELETE, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.UPDATE, 1);
    }

    @Test
    void givenMultipleListeners_eachShouldReceiveSeparateEvents() throws Exception {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        TestChangeStreamListener listener1 = new TestChangeStreamListener();
        TestChangeStreamListener listener2 = new TestChangeStreamListener();
        manager.registerListener(listener0, List.of(0));
        manager.registerListener(listener1, List.of(1));
        manager.registerListener(listener2, List.of(2));
        manager.start();

        insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        List<ChangeStreamDocument<Document>> events0 = listener0.getEvents();
        assertEquals(1, events0.size());
        assertEquals(OperationType.INSERT, events0.get(0).getOperationType());
        assertEquals(0, events0.get(0).getFullDocument().getInteger("testValue"));

        List<ChangeStreamDocument<Document>> events1 = listener1.getEvents();
        assertEquals(1, events1.size());
        assertEquals(OperationType.INSERT, events1.get(0).getOperationType());
        assertEquals(1, events1.get(0).getFullDocument().getInteger("testValue"));

        List<ChangeStreamDocument<Document>> events2 = listener2.getEvents();
        assertEquals(1, events2.size());
        assertEquals(OperationType.INSERT, events2.get(0).getOperationType());
        assertEquals(2, events2.get(0).getFullDocument().getInteger("testValue"));
    }

    @Test
    void shouldNotHandleEventsWhenListenerIsDeregisteredFromAllPartitions() throws Exception {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener0);
        manager.start();

        var numberOfInsertedDocuments = insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        List<ChangeStreamDocument<Document>> events0 = listener0.getEvents();
        assertEquals(numberOfInsertedDocuments, events0.size());

        // when
        manager.deregisterListenerFromAllPartitions(listener0);

        // then
        var deleteResult0 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_0_ID)));
        var deleteResult1 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_1_ID)));
        var deleteResult2 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_2_ID)));
        assertEquals(1, deleteResult0.getDeletedCount());
        assertEquals(1, deleteResult1.getDeletedCount());
        assertEquals(1, deleteResult2.getDeletedCount());

        // Wait for CDC event to arrive
        Thread.sleep(500);

        // only old events are available
        List<ChangeStreamDocument<Document>> events1 = listener0.getEvents();
        assertEquals(events0.size(), events1.size());
    }

    @Test
    void shouldNotHandleEventsFromSelectedPartitionWhenListenerIsDeregisteredFromThisPartition() throws Exception {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener0);
        manager.start();

        var numberOfInsertedDocuments = insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        List<ChangeStreamDocument<Document>> events0 = listener0.getEvents();
        assertEquals(numberOfInsertedDocuments, events0.size());

        // when
        manager.deregisterListener(listener0, List.of(0));

        // then
        var deleteResult0 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_0_ID)));
        var deleteResult1 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_1_ID)));
        var deleteResult2 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_2_ID)));
        assertEquals(1, deleteResult0.getDeletedCount());
        assertEquals(1, deleteResult1.getDeletedCount());
        assertEquals(1, deleteResult2.getDeletedCount());

        // Wait for CDC event to arrive
        Thread.sleep(500);

        // only old events and deletes from not deregistered partitions are available
        var numberOfExpectedDeleteEventsOnListeners = 2;
        var numberOfExpectedEvents = numberOfInsertedDocuments + numberOfExpectedDeleteEventsOnListeners;
        List<ChangeStreamDocument<Document>> events1 = listener0.getEvents();
        assertEquals(numberOfExpectedEvents, events1.size());
    }

    @Test
    void shouldGracefullyStopAllRegisteredListeners() throws Exception {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        TestChangeStreamListener listener1 = new TestChangeStreamListener();
        TestChangeStreamListener listener2 = new TestChangeStreamListener();
        manager.registerListener(listener0, List.of(0));
        manager.registerListener(listener1, List.of(1));
        manager.registerListener(listener2, List.of(2));
        manager.start();

        insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        assertDoesNotThrow(manager::stop);
    }

    @Test
    void givenConfigurationWithUserDefinedKey_eachListenerShouldReceiveSeparateEvents() throws Exception {
        var configWithUserDefinedKey = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .keyName("testId")
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();
        MongoCseManager manager = new MongoCseManager(configWithUserDefinedKey);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        TestChangeStreamListener listener1 = new TestChangeStreamListener();
        TestChangeStreamListener listener2 = new TestChangeStreamListener();
        manager.registerListener(listener0, List.of(0));
        manager.registerListener(listener1, List.of(1));
        manager.registerListener(listener2, List.of(2));
        manager.start();

        insertDocumentsWithTestIdToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        List<ChangeStreamDocument<Document>> events0 = listener0.getEvents();
        assertEquals(1, events0.size());
        assertEquals(OperationType.INSERT, events0.get(0).getOperationType());
        assertEquals(0, events0.get(0).getFullDocument().getInteger("testValue"));

        List<ChangeStreamDocument<Document>> events1 = listener1.getEvents();
        assertEquals(1, events1.size());
        assertEquals(OperationType.INSERT, events1.get(0).getOperationType());
        assertEquals(1, events1.get(0).getFullDocument().getInteger("testValue"));

        List<ChangeStreamDocument<Document>> events2 = listener2.getEvents();
        assertEquals(1, events2.size());
        assertEquals(OperationType.INSERT, events2.get(0).getOperationType());
        assertEquals(2, events2.get(0).getFullDocument().getInteger("testValue"));
    }

    private int insertDocumentsToAllPartitions() {
        Document testDoc0 = new Document(Map.of(
                "_id", new ObjectId(TestIds.MOD_0_ID),
                "testValue", 0
        ));
        Document testDoc1 = new Document(Map.of(
                "_id", new ObjectId(TestIds.MOD_1_ID),
                "testValue", 1
        ));
        Document testDoc2 = new Document(Map.of(
                "_id", new ObjectId(TestIds.MOD_2_ID),
                "testValue", 2
        ));
        var documentsToInsert = List.of(testDoc0, testDoc1, testDoc2);
        var result = collection.insertMany(documentsToInsert);
        return result.getInsertedIds().size();
    }

    private int insertDocumentsWithTestIdToAllPartitions() {
        Document testDoc0 = new Document(Map.of(
                "testId", new ObjectId(TestIds.MOD_0_ID),
                "testValue", 0
        ));
        Document testDoc1 = new Document(Map.of(
                "testId", new ObjectId(TestIds.MOD_1_ID),
                "testValue", 1
        ));
        Document testDoc2 = new Document(Map.of(
                "testId", new ObjectId(TestIds.MOD_2_ID),
                "testValue", 2
        ));
        var documentsToInsert = List.of(testDoc0, testDoc1, testDoc2);
        var result = collection.insertMany(documentsToInsert);
        return result.getInsertedIds().size();
    }

    private void assertEventsContainNumberOfOpTypes(List<ChangeStreamDocument<Document>> events, OperationType operationType, long expectedCount) {
        long actualCount = events.stream().filter(op -> op.getOperationType() == operationType).count();
        assertEquals(expectedCount, actualCount);
    }
}
