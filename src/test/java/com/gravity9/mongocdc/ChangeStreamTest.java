package com.gravity9.mongocdc;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChangeStreamTest {

	private final String URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority";

	private final String DB_NAME = "test";

	private final String COLL_NAME = "change_stream_test";

	@Test
	void givenProperConfiguration_shouldStartManagerAndRegisterListener() throws Exception {
		int partitionNumbers = 3;

		MongoCDCManager manager = new MongoCDCManager(URI, DB_NAME, COLL_NAME, partitionNumbers);

		TestChangeStreamListener listener = new TestChangeStreamListener();
		manager.registerListenerToAllPartitions(listener);
		manager.start();

		var collection = MongoClientProvider.createClient(URI).getDatabase(DB_NAME).getCollection(COLL_NAME);
		Document testDoc = new Document("testValue", 123);
		collection.insertOne(testDoc);

		// Wait for CDC event to arrive
		Thread.sleep(500);

		List<ChangeStreamDocument<Document>> events = listener.getEvents();
		assertEquals(1, events.size());
		assertEquals(OperationType.INSERT, events.get(0).getOperationType());
		assertEquals(123, events.get(0).getFullDocument().getInteger("testValue"));
	}
}
