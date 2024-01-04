package com.gravity9.mongocdc;

import com.gravity9.mongocdc.listener.ChangeStreamListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MongoCDCManager {

	private static final Logger log = LoggerFactory.getLogger(MongoCDCManager.class);

	private final ConfigManager configManager;

	private final MongoConfig mongoConfig;

	private final WorkerClusterConfig clusterConfig;

	private final Map<Integer, MongoChangeStreamWorker> workers;

	public MongoCDCManager(MongoConfig mongoConfig) {
		this.mongoConfig = mongoConfig;
		this.configManager = new ConfigManager(mongoConfig);
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
		Map<Integer, MongoChangeStreamWorker> workers = new HashMap<>();
		log.info("Creating workers for {} partitions for collection {}", partitions, collectionName);
		for (int partition = 0; partition < partitions; partition++) {
			workers.put(partition, new MongoChangeStreamWorker(
				mongoConfig,
				configManager,
				partition
			));
		}

		return workers;
	}

	/**
	 * Starts all workers for the specified collection in the MongoDB change data capture (CDC) manager.
	 * The workers will begin listening for change events and processing them accordingly.
	 * This method blocks until all workers are initialized and ready to process change events.
	 *
	 * @throws StartFailureException if an exception occurs during the start process
	 */
	public void start() {
		try {
			log.info("Starting all workers for collection {}", clusterConfig.getCollection());
			workers.values().forEach(MongoChangeStreamWorker::start);
			workers.values().forEach(MongoChangeStreamWorker::awaitInitialization);
			log.info("All workers for collection {} are now ready!", clusterConfig.getCollection());
		} catch (Exception ex) {
			try {
				stop();
			} catch (Exception exception2) {
				log.error("Stop on exception failed", exception2);
			}
			throw StartFailureException.startFailure(ex);
		}
	}

	/**
	 * Stops all workers for the specified collection in the MongoDB change data capture (CDC) manager.
	 * This method stops the workers from listening for change events and processing them.
	 * It also sets the thread reference to null, indicating that the worker has been stopped.
	 *
	 * @throws NullPointerException if the thread is null
	 */
	public void stop() {
		log.info("Starting all workers for collection {}", clusterConfig.getCollection());
		workers.values().forEach(MongoChangeStreamWorker::stop);
		log.info("All workers for collection {} are now ready!", clusterConfig.getCollection());
	}

	public void registerListener(ChangeStreamListener listener, Collection<Integer> partitions) {
		partitions.forEach(partition -> {
			MongoChangeStreamWorker worker = workers.get(partition);
			if (worker == null) {
				log.warn("Could not find worker for partition {} - cannot register listener {}", partition, listener.getClass().getName());
				return;
			}

			worker.register(listener);
		});
	}

	public void registerListenerToAllPartitions(ChangeStreamListener listener) {
		workers.values().forEach(worker -> worker.register(listener));
	}
}
