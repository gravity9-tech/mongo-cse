package com.gravity9.mongocdc;

import com.gravity9.mongocdc.listener.ChangeStreamListener;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public void start() {
		log.info("Starting all workers for collection {}", clusterConfig.getCollection());
		workers.values().forEach(MongoChangeStreamWorker::start);
		log.info("All workers for collection {} are now ready!", clusterConfig.getCollection());
	}

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
