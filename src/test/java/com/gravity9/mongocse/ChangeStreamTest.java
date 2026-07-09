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

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void givenConfigurationWithMatchStageWithSingleCondition_shouldAcceptOnlyMatchingEvents() throws InterruptedException {
        var match = Filters.gt("fullDocument.testValue", 0);
        var config = buildMongoConfig(match);

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        assertEquals(2, listener.getEvents().size());
    }

    @Test
    void givenConfigurationWithMatchStageWithMultipleConditions_shouldAcceptOnlyMatchingEvents() throws InterruptedException {
        var match = Filters.and(
                Filters.gt("fullDocument.testValue", 0),
                Filters.lt("fullDocument.testValue", 2),
                Filters.in("operationType", List.of("insert"))
        );
        var config = buildMongoConfig(match);

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        assertEquals(1, listener.getEvents().size());
    }

    @Test
    void givenConfigurationWithMatchStageWithIdFilter_shouldAcceptOnlyMatchingEvents() throws InterruptedException {
        var match = Filters.eq("fullDocument._id", new ObjectId(TestIds.MOD_0_ID));
        var config = buildMongoConfig(match);

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        insertDocumentsToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        assertEquals(1, listener.getEvents().size());
        assertEquals(0, listener.getEvents().get(0).getFullDocument().getInteger("testValue"));
    }

    @Test
    void givenConfigurationWithMatchStage_and_multipleListeners_shouldAcceptOnlyMatchingEvents() throws InterruptedException {
        var match = Filters.in("fullDocument.testValue", List.of(0, 2));
        var config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .match(match)
                .keyName("testId")
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

        insertDocumentsWithTestIdToAllPartitions();

        // Wait for CDC event to arrive
        Thread.sleep(500);

        List<ChangeStreamDocument<Document>> events0 = listener0.getEvents();
        List<ChangeStreamDocument<Document>> events1 = listener1.getEvents();
        List<ChangeStreamDocument<Document>> events2 = listener2.getEvents();

        assertEquals(1, events0.size());
        assertEquals(0, events0.get(0).getFullDocument().getInteger("testValue"));

        assertEquals(0, events1.size());

        assertEquals(1, events2.size());
        assertEquals(2, events2.get(0).getFullDocument().getInteger("testValue"));
    }

    @Test
    void givenDuplicateListener_shouldReceiveEventsIndependently() throws InterruptedException {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        // Create the primary listener and duplicate listener.
        TestChangeStreamListener primaryListener = new TestChangeStreamListener();
        TestChangeStreamListener duplicateListener = new TestChangeStreamListener();

        // Register the primary listener for partition 0 and duplicate for the same partition.
        manager.registerListener(primaryListener, List.of(0));
        manager.registerListener(duplicateListener, List.of(0));

        // Start the manager and allow time for listeners to initialize.
        manager.start();
        Thread.sleep(1000);

        // Insert a document into the collection, triggering an event for partition 0.
        Document testDoc0 = new Document(Map.of(
                "_id", new ObjectId(TestIds.MOD_0_ID),
                "testValue", 0
        ));
        collection.insertOne(testDoc0);

        // Wait for CDC events to be picked up by the listeners.
        Thread.sleep(500);

        // Retrieve events for both listeners.
        List<ChangeStreamDocument<Document>> primaryEvents = primaryListener.getEvents();
        List<ChangeStreamDocument<Document>> duplicateEvents = duplicateListener.getEvents();

        // Assert that both listeners receive the event independently.
        assertEquals(1, primaryEvents.size(), "Primary listener should receive 1 event.");
        assertEquals(1, duplicateEvents.size(), "Duplicate listener should receive 1 event.");

        // Verify that both listeners received the same event data.
        assertNotEquals(primaryEvents.get(0).getFullDocument(), null);
        assertEquals(0, primaryEvents.get(0).getFullDocument().getInteger("testValue"));
        assertEquals(0, duplicateEvents.get(0).getFullDocument().getInteger("testValue"));
    }

    @Test
    void givenAfterAllListenersAreDeregistered_shouldStopProcessingEvents() throws InterruptedException {
        // Arrange: Create a MongoCseManager and register two listeners for partition 0 and 1
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        TestChangeStreamListener listener0 = new TestChangeStreamListener();
        TestChangeStreamListener listener1 = new TestChangeStreamListener();
        manager.registerListener(listener0, List.of(0));
        manager.registerListener(listener1, List.of(1));
        manager.start();

        // Insert a document to trigger an event for both listeners
        insertDocumentsToAllPartitions();
        Thread.sleep(500);

        // Verify that listeners received initial events
        assertEquals(1, listener0.getEvents().size(), "Listener 0 should receive 1 event.");
        assertEquals(1, listener1.getEvents().size(), "Listener 1 should receive 1 event.");

        // Act: Deregister both listeners from all partitions
        manager.deregisterListenerFromAllPartitions(listener0);
        manager.deregisterListenerFromAllPartitions(listener1);

        // Insert another documents to test if deregistered listeners receive any events
        insertMultipleDocumentsToAllPartitions(5);
        Thread.sleep(500);

        // Assert: Verify that no new events are received by either listener
        assertEquals(1, listener0.getEvents().size(), "Listener 0 should not receive any new events.");
        assertEquals(1, listener1.getEvents().size(), "Listener 1 should not receive any new events.");
    }

    @Test
    void givenListenerIsDeregistered_shouldNotHandleEventsFromSpecificPartition() throws InterruptedException {
        // Given: Create a MongoCseManager with a configuration
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        // Register a single listener to all partitions (0, 1, and 2)
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        // Insert documents into all partitions
        var numberOfInsertedDocuments = insertDocumentsToAllPartitions();

        // Wait for CDC events to be picked up
        Thread.sleep(500);

        // Verify that the listener received all events for all partitions
        List<ChangeStreamDocument<Document>> initialEvents = listener.getEvents();
        assertEquals(numberOfInsertedDocuments, initialEvents.size());

        // When: Deregister the listener from partition 1 only
        manager.deregisterListener(listener, List.of(1));

        // Perform a new operation on each partition
        var deleteResult0 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_0_ID)));
        var deleteResult1 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_1_ID)));
        var deleteResult2 = collection.deleteOne(Filters.eq("_id", new ObjectId(TestIds.MOD_2_ID)));

        assertEquals(1, deleteResult0.getDeletedCount());
        assertEquals(1, deleteResult1.getDeletedCount());
        assertEquals(1, deleteResult2.getDeletedCount());

        // Wait for CDC events to be picked up
        Thread.sleep(500);

        // Then: Only events from partition 0 and 2 should be handled (since partition 1 was deregistered)
        var expectedNewEventCount = 2; // 1 delete event from partition 0 and 1 delete event from partition 2
        var totalExpectedEvents = numberOfInsertedDocuments + expectedNewEventCount;

        List<ChangeStreamDocument<Document>> finalEvents = listener.getEvents();
        assertEquals(totalExpectedEvents, finalEvents.size());
    }

    @Test
    void givenListenerOnSpecificPartition_shouldNotReceiveEventsFromOtherPartitions() throws InterruptedException {
        MongoCseManager manager = new MongoCseManager(mongoConfig);

        // Create a listener and register it only for partition 1.
        TestChangeStreamListener partition1Listener = new TestChangeStreamListener();
        manager.registerListener(partition1Listener, List.of(1));

        // Start the manager and allow time for listeners to initialize.
        manager.start();
        Thread.sleep(1000);

        // Insert a document in partition 0, which should not trigger an event for partition 1.
        Document testDocPartition0 = new Document(Map.of(
                "_id", new ObjectId(TestIds.MOD_0_ID),
                "testValue", 0
        ));
        collection.insertOne(testDocPartition0);

        // Wait for CDC events to be picked up by the listener.
        Thread.sleep(500);

        // Verify that partition1Listener did not receive any events.
        List<ChangeStreamDocument<Document>> partition1Events = partition1Listener.getEvents();
        assertEquals(0, partition1Events.size(), "Listener registered for partition 1 should not receive events from partition 0.");
    }

    private MongoConfig buildMongoConfig(Bson match) {
        return MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .match(match)
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .build();
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

    private void insertMultipleDocumentsToAllPartitions(int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            Document testDoc = new Document(Map.of(
                    "testId", new ObjectId(TestIds.MOD_3_ID),
                    "testValue", i
            ));
            collection.insertOne(testDoc);
        }
    }

    private void assertEventsContainNumberOfOpTypes(List<ChangeStreamDocument<Document>> events, OperationType operationType, long expectedCount) {
        long actualCount = events.stream().filter(op -> op.getOperationType() == operationType).count();
        assertEquals(expectedCount, actualCount);
    }

    @Test
    void givenConfigurationWithFieldNamesFilter_shouldAcceptOnlyEventsWithSpecifiedFields() throws Exception {
        var config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .fieldNames("name", "status")
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        Document testDoc = new Document(Map.of(
                "name", "John Doe",
                "status", "active",
                "description", "Initial description"
        ));
        InsertOneResult insertResult = collection.insertOne(testDoc);
        ObjectId docId = insertResult.getInsertedId().asObjectId().getValue();
        log.info("Inserted document: {}", docId.toHexString());

        collection.updateOne(
                Filters.eq("_id", docId),
                Updates.set("name", "Jane Doe")
        );

        collection.updateOne(
                Filters.eq("_id", docId),
                Updates.set("description", "Updated description")
        );

        collection.updateOne(
                Filters.eq("_id", docId),
                Updates.set("status", "inactive")
        );

        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 2);

        assertEquals(2, events.size());
        assertEventsContainNumberOfOpTypes(events, OperationType.UPDATE, 2);

        var firstUpdate = events.get(0).getUpdateDescription();
        var secondUpdate = events.get(1).getUpdateDescription();

        assertDoesNotThrow(() -> {
            boolean hasNameUpdate = (firstUpdate != null && firstUpdate.getUpdatedFields().containsKey("name")) ||
                    (secondUpdate != null && secondUpdate.getUpdatedFields().containsKey("name"));
            boolean hasStatusUpdate = (firstUpdate != null && firstUpdate.getUpdatedFields().containsKey("status")) ||
                    (secondUpdate != null && secondUpdate.getUpdatedFields().containsKey("status"));
            assertEquals(true, hasNameUpdate, "Expected update event for 'name' field");
            assertEquals(true, hasStatusUpdate, "Expected update event for 'status' field");
        });
    }

    @Test
    void givenConfigurationWithSingleOperationType_shouldAcceptOnlyMatchingEvents() throws Exception {
        var config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .operationTypes(OperationType.INSERT)
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        Document testDoc = new Document("testValue", 123);
        InsertOneResult insertResult = collection.insertOne(testDoc);
        ObjectId docId = insertResult.getInsertedId().asObjectId().getValue();

        collection.updateOne(Filters.eq("_id", docId), Updates.set("testValue", 456));
        collection.deleteOne(Filters.eq("_id", docId));

        Thread.sleep(500);

        List<ChangeStreamDocument<Document>> events = listener.getEvents();
        assertEquals(1, events.size());
        assertEventsContainNumberOfOpTypes(events, OperationType.INSERT, 1);
    }

    @Test
    void givenConfigurationWithMultipleOperationTypes_shouldAcceptOnlyMatchingEvents() throws Exception {
        var config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .operationTypes(OperationType.INSERT, OperationType.DELETE)
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        Document testDoc = new Document("testValue", 123);
        InsertOneResult insertResult = collection.insertOne(testDoc);
        ObjectId docId = insertResult.getInsertedId().asObjectId().getValue();

        collection.updateOne(Filters.eq("_id", docId), Updates.set("testValue", 456));

        collection.deleteOne(Filters.eq("_id", docId));

        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 2);

        assertEquals(2, events.size());
        assertEventsContainNumberOfOpTypes(events, OperationType.INSERT, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.DELETE, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.UPDATE, 0);
    }

    @Test
    void givenConfigurationWithNullOperationTypes_shouldAcceptAllEvents() throws Exception {
        var config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .operationTypes((OperationType[]) null)
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        Document testDoc = new Document("testValue", 123);
        InsertOneResult insertResult = collection.insertOne(testDoc);
        ObjectId docId = insertResult.getInsertedId().asObjectId().getValue();

        collection.updateOne(Filters.eq("_id", docId), Updates.set("testValue", 456));
        collection.deleteOne(Filters.eq("_id", docId));

        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 3);

        assertEquals(3, events.size());
        assertEventsContainNumberOfOpTypes(events, OperationType.INSERT, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.UPDATE, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.DELETE, 1);
    }

    @Test
    void givenConfigurationWithEmptyOperationTypes_shouldAcceptAllEvents() throws Exception {
        var config = MongoConfig.builder()
                .connectionUri(getConnectionUri())
                .databaseName(getDatabaseName())
                .collectionName(getTestCollectionName())
                .workerConfigCollectionName(getWorkerConfigCollectionName())
                .clusterConfigCollectionName(getClusterConfigCollectionName())
                .numberOfPartitions(3)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .operationTypes()
                .build();

        MongoCseManager manager = new MongoCseManager(config);
        TestChangeStreamListener listener = new TestChangeStreamListener();
        manager.registerListenerToAllPartitions(listener);
        manager.start();

        Thread.sleep(1000);

        Document testDoc = new Document("testValue", 123);
        InsertOneResult insertResult = collection.insertOne(testDoc);
        ObjectId docId = insertResult.getInsertedId().asObjectId().getValue();

        collection.updateOne(Filters.eq("_id", docId), Updates.set("testValue", 456));
        collection.deleteOne(Filters.eq("_id", docId));

        List<ChangeStreamDocument<Document>> events = waitForEvents(listener, 3);

        assertEquals(3, events.size());
        assertEventsContainNumberOfOpTypes(events, OperationType.INSERT, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.UPDATE, 1);
        assertEventsContainNumberOfOpTypes(events, OperationType.DELETE, 1);
    }
}
