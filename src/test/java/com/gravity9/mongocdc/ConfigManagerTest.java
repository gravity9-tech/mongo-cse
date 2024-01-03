package com.gravity9.mongocdc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigManagerTest extends AbstractMongoDbBase {

	private MongoConfig.MongoConfigBuilder mongoConfigBuilder = new MongoConfig.MongoConfigBuilder()
			.connectionUri("mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority")
			.databaseName("test-db")
			.collectionName("example")
			.workerConfigCollectionName("changeStreamWorkerConfig")
			.clusterConfigCollectionName("changeStreamClusterConfig");

	@Test
	void givenSameConfiguration_shouldNotThrowException() {
		int partitions = 3;
		var mongoConfig = mongoConfigBuilder
				.numberOfPartitions(partitions)
				.build();
		new MongoCDCManager(mongoConfig);
		assertDoesNotThrow(() -> new MongoCDCManager(mongoConfig));

		WorkerClusterConfig config = new ConfigManager(mongoConfig).getOrInitClusterConfig(COLL_NAME, partitions);
		assertEquals(COLL_NAME, config.getCollection());
		assertEquals(partitions, config.getPartitions());
	}

	@Test
	void givenNewConfigWithDifferentNumberOfPartitions_shouldThrowException() {
		var firstConfig = mongoConfigBuilder
				.numberOfPartitions(3)
				.build();
		var secondConfig = mongoConfigBuilder
				.numberOfPartitions(1)
				.build();
		new MongoCDCManager(firstConfig);
		assertThrows(IllegalArgumentException.class, () -> new MongoCDCManager(secondConfig));
	}
}
