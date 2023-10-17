package com.gravity9.mongocdc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.util.List;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WorkerConfigManager {

	private static final Logger log = LoggerFactory.getLogger(WorkerConfigManager.class);

	private static final String COLLECTION_NAME = "changeStreamWorkerConfig";

	private final MongoCollection<ChangeStreamWorkerConfig> configCollection;

	WorkerConfigManager(String uri, String databaseName) {
		MongoClient client = MongoClientProvider.createClient(uri);
		MongoDatabase db = client.getDatabase(databaseName);
		configCollection = db.getCollection(COLLECTION_NAME, ChangeStreamWorkerConfig.class);
	}

	ChangeStreamWorkerConfig getConfigOrInit(String listenedCollection, Integer partition) {
		ChangeStreamWorkerConfig changeStreamWorkerConfig = configCollection.find(Filters.and(List.of(
			Filters.eq("collection", listenedCollection),
			Filters.eq("partition", partition)
		))).first();

		if (changeStreamWorkerConfig == null) {
			log.info("Creating new config for partition {} on collection {}", partition, listenedCollection);
			changeStreamWorkerConfig = new ChangeStreamWorkerConfig();
			changeStreamWorkerConfig.setCollection(listenedCollection);
			changeStreamWorkerConfig.setPartition(partition);

			configCollection.insertOne(changeStreamWorkerConfig);

			changeStreamWorkerConfig = configCollection.find(Filters.and(List.of(
				Filters.eq("collection", listenedCollection),
				Filters.eq("partition", partition)
			))).first();
		}

		return changeStreamWorkerConfig;
	}

	ChangeStreamWorkerConfig updateResumeToken(ObjectId id, String resumeToken) {
		configCollection.updateOne(
			Filters.eq("_id", id),
			Updates.set("resumeToken", resumeToken)
		);

		return configCollection.find(Filters.eq("_id")).first();
	}
}
