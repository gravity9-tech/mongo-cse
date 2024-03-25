package com.gravity9.mongocse;

import com.gravity9.mongocse.listener.ChangeStreamListener;
import com.gravity9.mongocse.logging.LoggingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MongoCseManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(MongoCseManager.class);

    private final ConfigManager configManager;

    private final MongoConfig mongoConfig;

    private final WorkerClusterConfig clusterConfig;

    private final Map<Integer, MongoChangeStreamWorker> workers;

    private final String managerId;

    private final MongoClientProvider clientProvider;

    public MongoCseManager(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        this.managerId = LoggingUtil.createManagerId(mongoConfig);
        this.clientProvider = new MongoClientProvider(mongoConfig.getConnectionUri());
        this.configManager = new ConfigManager(mongoConfig, this.clientProvider);
        configManager.verifyClusterConfig(mongoConfig.getCollectionName(), mongoConfig.getNumberOfPartitions());
        this.clusterConfig = configManager.getOrInitClusterConfig(mongoConfig.getCollectionName(), mongoConfig.getNumberOfPartitions());
        this.workers = createWorkers();
    }

    private Map<Integer, MongoChangeStreamWorker> createWorkers() {
        var partitions = clusterConfig.getPartitions();
        if (partitions < 1) {
            throw new IllegalArgumentException("Cannot initialize with less than 1 partition!");
        }

        var collectionName = clusterConfig.getCollection();
        Map<Integer, MongoChangeStreamWorker> workersByPartitionMap = new HashMap<>();
        log.info("{} - Creating workers for {} partitions for collection {}", managerId, partitions, collectionName);
        for (int partition = 0; partition < partitions; partition++) {
            workersByPartitionMap.put(partition, new MongoChangeStreamWorker(
                    mongoConfig,
                    configManager,
                    partition,
                    managerId,
                    clientProvider
            ));
        }

        return workersByPartitionMap;
    }

    /**
     * Starts all workers for the specified collection in the MongoDB change stream enhancer (CSE) manager.
     * The workers will begin listening for change events and processing them accordingly.
     * This method blocks until all workers are initialized and ready to process change events.
     *
     * @throws StartFailureException if an exception occurs during the start process
     */
    public void start() {
        try {
            log.info("{} - Starting all workers for collection {}", managerId, clusterConfig.getCollection());
            workers.values().forEach(worker -> {
                runInNewThread(worker);
                worker.awaitInitialization();
            });
            log.info("{} - All workers for collection {} are now ready!", managerId, clusterConfig.getCollection());
        } catch (Exception ex) {
            try {
                stop();
            } catch (Exception exception2) {
                log.error("{} - Stop on exception failed", managerId, exception2);
            }
            throw StartFailureException.startFailure(ex);
        }
    }

    /**
     * Stops all workers for the specified collection in the MongoDB change stream enhancer (CSE) manager.
     * This method stops the workers from listening for change events and processing them.
     * It also sets the thread reference to null, indicating that the worker has been stopped.
     *
     * @throws NullPointerException if the thread is null
     */
    public void stop() {
        log.info("{} - Stopping all workers for collection {}", managerId, clusterConfig.getCollection());
        workers.values().forEach(MongoChangeStreamWorker::stop);
        log.info("{} - All workers for collection {} are now stopped!", managerId, clusterConfig.getCollection());
    }

    public void registerListener(ChangeStreamListener listener, Collection<Integer> partitions) {
        partitions.forEach(partition -> {
            var workerOptional = getMongoChangeStreamWorker(listener, partition);
            workerOptional.ifPresent(worker -> worker.register(listener));
        });
    }

    public void registerListenerToAllPartitions(ChangeStreamListener listener) {
        workers.values().forEach(worker -> worker.register(listener));
    }

    public void deregisterListener(ChangeStreamListener listener, Collection<Integer> partitions) {
        partitions.forEach(partition -> {
            var workerOptional = getMongoChangeStreamWorker(listener, partition);
            workerOptional.ifPresent(worker -> deregister(worker, listener));
        });
    }

    public void deregisterListenerFromAllPartitions(ChangeStreamListener listener) {
        workers.values().forEach(worker -> worker.deregister(listener));
    }

    private void deregister(MongoChangeStreamWorker worker, ChangeStreamListener listener) {
        worker.deregister(listener);
    }

    private Optional<MongoChangeStreamWorker> getMongoChangeStreamWorker(ChangeStreamListener listener, Integer partition) {
        MongoChangeStreamWorker worker = workers.get(partition);
        if (worker == null) {
            log.warn("{} - Could not find worker for partition {} - cannot register listener {}", managerId, partition, listener);
            return Optional.empty();
        }
        return Optional.of(worker);
    }

    private void runInNewThread(MongoChangeStreamWorker worker) {
        Thread thread = new Thread(worker);
        thread.start();
    }

    @Override
    public void close() throws IOException {
        clientProvider.close();
    }
}
