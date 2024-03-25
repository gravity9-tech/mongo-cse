package com.gravity9.mongocse;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConfigManager {

	private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

	private final MongoCollection<ChangeStreamWorkerConfig> workerConfigCollection;

	private final MongoCollection<WorkerClusterConfig> clusterConfigCollection;

	ConfigManager(MongoConfig mongoConfig, MongoClientProvider clientProvider) {
		MongoDatabase db = clientProvider.getClient().getDatabase(mongoConfig.getDatabaseName());
		workerConfigCollection = db.getCollection(mongoConfig.getWorkerConfigCollectionName(), ChangeStreamWorkerConfig.class);
		clusterConfigCollection = db.getCollection(mongoConfig.getClusterConfigCollectionName(), WorkerClusterConfig.class);
	}

	void verifyClusterConfig(String collectionName, Integer partitions) {
		WorkerClusterConfig clusterConfig = findConfig(collectionName).orElse(null);
		if (clusterConfig == null) {
			return;
		}

		if (clusterConfig.getPartitions() == partitions) {
			return;
		}

		String message = String.format(
			"Found previous config for collection %s with different number of partitions! In existing config: %d, in requested config: %d",
			collectionName,
			clusterConfig.getPartitions(),
			partitions
		);

		throw new IllegalArgumentException(message);
	}

	WorkerClusterConfig getOrInitClusterConfig(String collectionName, Integer partitions) {
		return findConfig(collectionName)
			.orElseGet(() -> {
				log.info("Creating new cluster config for collection {}", collectionName);
				var clusterConfig = new WorkerClusterConfig();
				clusterConfig.setCollection(collectionName);
				clusterConfig.setPartitions(partitions);
				clusterConfigCollection.insertOne(clusterConfig);
				return findConfig(collectionName).orElseThrow(() -> new IllegalStateException("Could not create config for collection: " + collectionName));
			});
	}

	private Optional<WorkerClusterConfig> findConfig(String collectionName) {
		return Optional.ofNullable(clusterConfigCollection.find(Filters.and(List.of(
			Filters.eq("collection", collectionName)
		))).first());
	}

	ChangeStreamWorkerConfig getConfigOrInit(String listenedCollection, Integer partition) {
		ChangeStreamWorkerConfig changeStreamWorkerConfig = workerConfigCollection.find(Filters.and(List.of(
			Filters.eq("collection", listenedCollection),
			Filters.eq("partition", partition)
		))).first();

		if (changeStreamWorkerConfig == null) {
			log.info("Creating new config for partition {} on collection {}", partition, listenedCollection);
			changeStreamWorkerConfig = new ChangeStreamWorkerConfig();
			changeStreamWorkerConfig.setCollection(listenedCollection);
			changeStreamWorkerConfig.setPartition(partition);

			workerConfigCollection.insertOne(changeStreamWorkerConfig);

			changeStreamWorkerConfig = workerConfigCollection.find(Filters.and(List.of(
				Filters.eq("collection", listenedCollection),
				Filters.eq("partition", partition)
			))).first();
		}

		return changeStreamWorkerConfig;
	}

	ChangeStreamWorkerConfig updateResumeToken(ObjectId id, String resumeToken) {
		workerConfigCollection.updateOne(
			Filters.eq("_id", id),
			Updates.set("resumeToken", resumeToken)
		);

		return workerConfigCollection.find(Filters.eq("_id")).first();
	}
}
