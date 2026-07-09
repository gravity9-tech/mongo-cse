package com.gravity9.mongocse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        new MongoCseManager(mongoConfig);
        assertDoesNotThrow(() -> new MongoCseManager(mongoConfig));

        WorkerClusterConfig config = new ConfigManager(mongoConfig, CLIENT_PROVIDER).getOrInitClusterConfig(getTestCollectionName(), partitions);
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
        new MongoCseManager(firstConfig);
        assertThrows(IllegalArgumentException.class, () -> new MongoCseManager(secondConfig));
    }

    @Test
    void givenNullConfig_shouldThrowNullPointerException() {
        assertThrows(NullPointerException.class, () -> new MongoCseManager(null));
    }

    @Test
    void givenWrongDatabaseName_shouldThrowException() {
        var mongoConfig = mongoConfigBuilder
                .databaseName("invalid_db_name")
                .build();
        assertThrows(IllegalArgumentException.class, () -> new MongoCseManager(mongoConfig));
    }

    @Test
    void givenInvalidConnectionUri_shouldThrowException() {
        var mongoConfig = mongoConfigBuilder
                .connectionUri("invalid_uri")
                .build();
        assertThrows(IllegalArgumentException.class, () -> new MongoCseManager(mongoConfig));
    }

    @Test
    void givenInvalidCollectionName_shouldThrowException() {
        var mongoConfig = mongoConfigBuilder
                .collectionName("invalid_collection_name")
                .build();
        assertThrows(IllegalArgumentException.class, () -> new MongoCseManager(mongoConfig));
    }

    @Test
    void givenSameConfigTwice_shouldCreateSeparateInstances() {
        var mongoConfig = mongoConfigBuilder.numberOfPartitions(2).build();
        MongoCseManager firstInstance = new MongoCseManager(mongoConfig);
        MongoCseManager secondInstance = new MongoCseManager(mongoConfig);
        assertNotSame(firstInstance, secondInstance);
    }
	@Test
	void givenNewConfigWithDifferentNumberOfPartitions_shouldThrowException() {
		var firstConfig = mongoConfigBuilder
				.numberOfPartitions(3)
				.build();
		var secondConfig = mongoConfigBuilder
				.numberOfPartitions(1)
				.build();
		new MongoCseManager(firstConfig);
		assertThrows(IllegalArgumentException.class, () -> new MongoCseManager(secondConfig));
	}

	@Test
	void givenWorkerConfigWithResumeToken_whenClearResumeToken_thenTokenIsRemoved() {
		int partitions = 1;
		var mongoConfig = mongoConfigBuilder
				.numberOfPartitions(partitions)
				.build();
		ConfigManager configManager = new ConfigManager(mongoConfig, CLIENT_PROVIDER);

		ChangeStreamWorkerConfig workerConfig = configManager.getConfigOrInit(getTestCollectionName(), 0);
		assertNotNull(workerConfig.getId());
		assertNull(workerConfig.getResumeToken());

		String testToken = "test-resume-token-12345";
		ChangeStreamWorkerConfig updatedConfig = configManager.updateResumeToken(workerConfig.getId(), testToken);
		assertEquals(testToken, updatedConfig.getResumeToken());

		configManager.clearResumeToken(workerConfig.getId());

		ChangeStreamWorkerConfig clearedConfig = configManager.getConfigOrInit(getTestCollectionName(), 0);
		assertNull(clearedConfig.getResumeToken());
	}

	@Test
	void givenMultipleWorkerConfigs_whenClearResumeToken_thenOnlyTargetConfigIsCleared() {
		int partitions = 3;
		var mongoConfig = mongoConfigBuilder
				.numberOfPartitions(partitions)
				.build();
		ConfigManager configManager = new ConfigManager(mongoConfig, CLIENT_PROVIDER);

		ChangeStreamWorkerConfig config0 = configManager.getConfigOrInit(getTestCollectionName(), 0);
		ChangeStreamWorkerConfig config1 = configManager.getConfigOrInit(getTestCollectionName(), 1);
		ChangeStreamWorkerConfig config2 = configManager.getConfigOrInit(getTestCollectionName(), 2);

		String token0 = "token-partition-0";
		String token1 = "token-partition-1";
		String token2 = "token-partition-2";

		configManager.updateResumeToken(config0.getId(), token0);
		configManager.updateResumeToken(config1.getId(), token1);
		configManager.updateResumeToken(config2.getId(), token2);

		configManager.clearResumeToken(config1.getId());

		ChangeStreamWorkerConfig refreshedConfig0 = configManager.getConfigOrInit(getTestCollectionName(), 0);
		ChangeStreamWorkerConfig refreshedConfig1 = configManager.getConfigOrInit(getTestCollectionName(), 1);
		ChangeStreamWorkerConfig refreshedConfig2 = configManager.getConfigOrInit(getTestCollectionName(), 2);

		assertEquals(token0, refreshedConfig0.getResumeToken());
		assertNull(refreshedConfig1.getResumeToken());
		assertEquals(token2, refreshedConfig2.getResumeToken());
	}
}
