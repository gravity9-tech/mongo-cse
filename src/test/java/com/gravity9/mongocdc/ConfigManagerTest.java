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

	@AfterEach
	void cleanUp() {
		TestMongoUtils.cleanUp();
	}

	@Test
	void givenSameConfiguration_shouldNotThrowException() {
		int partitions = 3;
		new MongoCDCManager(CONN_URI, DB_NAME, COLL_NAME, partitions);
		assertDoesNotThrow(() -> new MongoCDCManager(CONN_URI, DB_NAME, COLL_NAME, partitions));

		WorkerClusterConfig config = new ConfigManager(CONN_URI, DB_NAME).getOrInitClusterConfig(COLL_NAME, partitions);
		assertEquals(COLL_NAME, config.getCollection());
		assertEquals(partitions, config.getPartitions());
	}

	@Test
	void givenNewConfigWithDifferentNumberOfPartitions_shouldThrowException() {
		new MongoCDCManager(CONN_URI, DB_NAME, COLL_NAME, 3);
		assertThrows(IllegalArgumentException.class, () -> new MongoCDCManager(CONN_URI, DB_NAME, COLL_NAME, 1));
	}
}
