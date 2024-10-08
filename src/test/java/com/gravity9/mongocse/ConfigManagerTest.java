package com.gravity9.mongocse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
}
