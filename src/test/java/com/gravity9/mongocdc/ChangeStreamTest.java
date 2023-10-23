package com.gravity9.mongocdc;

import com.gravity9.mongocdc.constants.TestIds;
import com.gravity9.mongocdc.listener.TestChangeStreamListener;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.gravity9.mongocdc.constants.TestConstants.COLL_NAME;
import static com.gravity9.mongocdc.constants.TestConstants.CONN_URI;
import static com.gravity9.mongocdc.constants.TestConstants.DB_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChangeStreamTest {

	@AfterEach
	void cleanUp() {
		TestMongoUtils.cleanUp();
	}

	@Test
	void givenProperConfiguration_shouldStartManagerAndRegisterListener() throws Exception {
		int partitionNumbers = 3;

		MongoCDCManager manager = new MongoCDCManager(CONN_URI, DB_NAME, COLL_NAME, partitionNumbers);

		TestChangeStreamListener listener = new TestChangeStreamListener();
		manager.registerListenerToAllPartitions(listener);
		manager.start();

		var collection = MongoClientProvider.createClient(CONN_URI).getDatabase(DB_NAME).getCollection(COLL_NAME);
		Document testDoc = new Document("testValue", 123);
		collection.insertOne(testDoc);

		// Wait for CDC event to arrive
		Thread.sleep(500);

		List<ChangeStreamDocument<Document>> events = listener.getEvents();
		assertEquals(1, events.size());
		assertEquals(OperationType.INSERT, events.get(0).getOperationType());
		assertEquals(123, events.get(0).getFullDocument().getInteger("testValue"));
	}

	@Test
	void givenMultipleListeners_eachShouldReceiveSeparateEvents() throws Exception {
		int partitionNumbers = 3;

		MongoCDCManager manager = new MongoCDCManager(CONN_URI, DB_NAME, COLL_NAME, partitionNumbers);

		TestChangeStreamListener listener0 = new TestChangeStreamListener();
		TestChangeStreamListener listener1 = new TestChangeStreamListener();
		TestChangeStreamListener listener2 = new TestChangeStreamListener();
		manager.registerListener(listener0, List.of(0));
		manager.registerListener(listener1, List.of(1));
		manager.registerListener(listener2, List.of(2));
		manager.start();

		var collection = MongoClientProvider.createClient(CONN_URI).getDatabase(DB_NAME).getCollection(COLL_NAME);
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
		collection.insertOne(testDoc0);
		collection.insertOne(testDoc1);
		collection.insertOne(testDoc2);

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
}
