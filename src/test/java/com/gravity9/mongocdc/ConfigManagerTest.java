package com.gravity9.mongocdc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.gravity9.mongocdc.constants.TestConstants.COLL_NAME;
import static com.gravity9.mongocdc.constants.TestConstants.CONN_URI;
import static com.gravity9.mongocdc.constants.TestConstants.DB_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigManagerTest {

	private MongoConfig.MongoConfigBuilder mongoConfigBuilder = new MongoConfig.MongoConfigBuilder()
			.connectionUri("mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority")
			.databaseName("test-db")
			.collectionName("example")
			.workerConfigCollectionName("changeStreamWorkerConfig")
			.clusterConfigCollectionName("changeStreamClusterConfig");

	@AfterEach
	void cleanUp() {
		TestMongoUtils.cleanUp();
	}

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
