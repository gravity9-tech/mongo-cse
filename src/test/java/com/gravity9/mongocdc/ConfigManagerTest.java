package com.gravity9.mongocdc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigManagerTest extends AbstractMongoDbBase {

	@Test
	void givenSameConfiguration_shouldNotThrowException() {
		int partitions = 3;
		new MongoCDCManager(getConnectionUri(), getDatabaseName(), getTestCollectionName(), partitions);
		assertDoesNotThrow(() -> new MongoCDCManager(getConnectionUri(), getDatabaseName(), getTestCollectionName(), partitions));

		WorkerClusterConfig config = new ConfigManager(getConnectionUri(), getDatabaseName()).getOrInitClusterConfig(getTestCollectionName(), partitions);
		assertEquals(getTestCollectionName(), config.getCollection());
		assertEquals(partitions, config.getPartitions());
	}

	@Test
	void givenNewConfigWithDifferentNumberOfPartitions_shouldThrowException() {
		new MongoCDCManager(getConnectionUri(), getDatabaseName(), getTestCollectionName(), 3);
		assertThrows(IllegalArgumentException.class, () -> new MongoCDCManager(getConnectionUri(), getDatabaseName(), getTestCollectionName(), 1));
	}
}
