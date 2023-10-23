package com.gravity9.mongocdc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import static com.gravity9.mongocdc.constants.TestConstants.COLL_NAME;
import static com.gravity9.mongocdc.constants.TestConstants.CONN_URI;
import static com.gravity9.mongocdc.constants.TestConstants.DB_NAME;

public class TestMongoUtils {

	static void cleanUp() {
		MongoClient client = MongoClientProvider.createClient(CONN_URI);
		MongoDatabase db = client.getDatabase(DB_NAME);
		db.getCollection(COLL_NAME).drop();
		db.getCollection("changeStreamWorkerConfig")
			.deleteMany(Filters.eq("collection", COLL_NAME));
		db.getCollection("changeStreamClusterConfig")
			.deleteMany(Filters.eq("collection", COLL_NAME));
	}
}
