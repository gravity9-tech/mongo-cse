package com.gravity9.mongocdc;

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
		new MongoCDCManager(mongoConfig);
		assertDoesNotThrow(() -> new MongoCDCManager(mongoConfig));

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
		new MongoCDCManager(firstConfig);
		assertThrows(IllegalArgumentException.class, () -> new MongoCDCManager(secondConfig));
	}
}
