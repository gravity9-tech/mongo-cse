package com.gravity9.mongocse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigManagerTest extends AbstractMongoDbBase {

	private MongoConfig.MongoConfigBuilder mongoConfigBuilder;

	@BeforeEach
	public void setup() {
		super.setup();
		mongoConfigBuilder = new MongoConfig.MongoConfigBuilder()
				.connectionUri(getConnectionUri())
				.databaseName(getDatabaseName())
				.collectionName(getTestCollectionName())
				.workerConfigCollectionName(getWorkerConfigCollectionName())
				.clusterConfigCollectionName(getClusterConfigCollectionName());
	}

	@Test
	void givenSameConfiguration_shouldNotThrowException() {
		int partitions = 3;
		var mongoConfig = mongoConfigBuilder
				.numberOfPartitions(partitions)
				.build();
		new MongoCSEManager(mongoConfig);
		assertDoesNotThrow(() -> new MongoCSEManager(mongoConfig));

		WorkerClusterConfig config = new ConfigManager(mongoConfig).getOrInitClusterConfig(getTestCollectionName(), partitions);
		assertEquals(getTestCollectionName(), config.getCollection());
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
		new MongoCSEManager(firstConfig);
		assertThrows(IllegalArgumentException.class, () -> new MongoCSEManager(secondConfig));
	}
}
