package com.gravity9.mongocdc;

import com.gravity9.mongocdc.listener.ChangeStreamListener;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gravity9.mongocdc.MongoExpressions.divide;
import static com.gravity9.mongocdc.MongoExpressions.eq;
import static com.gravity9.mongocdc.MongoExpressions.expr;
import static com.gravity9.mongocdc.MongoExpressions.mod;
import static com.gravity9.mongocdc.MongoExpressions.toDate;
import static com.gravity9.mongocdc.MongoExpressions.toLong;


class MongoChangeStreamWorker implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(MongoChangeStreamWorker.class);

	private final String uri;
	private final String databaseName;
	private final String listenedCollection;
	private final int partitionNumbers;
	private final int partition;

	private final ConfigManager configManager;
	private final Set<ChangeStreamListener> listeners;
	private Thread thread = null;
	private String resumeToken;
	private ObjectId configId;

	MongoChangeStreamWorker(String uri,
							String databaseName,
							String listenedCollection,
							ConfigManager configManager,
							int partitionNumbers,
							int partition) {
		this.uri = uri;
		this.databaseName = databaseName;
		this.listenedCollection = listenedCollection;
		this.configManager = configManager;
		this.partitionNumbers = partitionNumbers;
		this.partition = partition;
		this.listeners = new HashSet<>();
	}

	public void start() {
		log.info("Starting worker for partition {} on collection '{}'", partition, listenedCollection);
		ChangeStreamWorkerConfig changeStreamWorkerConfig = configManager.getConfigOrInit(listenedCollection, partition);
		this.resumeToken = changeStreamWorkerConfig.getResumeToken();
		this.configId = changeStreamWorkerConfig.getId();

		this.thread = new Thread(this);
		this.thread.start();
		log.info("Worker for partition {} on collection {} is now started!", partition, listenedCollection);
	}

	public void stop() {
		this.thread.stop();
		this.thread = null;
		log.info("Worker for partition {} on collection {} stopped!", partition, listenedCollection);
	}

	@Override
	public void run() {
		MongoClient mongoClient = MongoClientProvider.createClient(uri);
		MongoDatabase db = mongoClient.getDatabase(databaseName);
		MongoCollection<Document> collection = db.getCollection(this.listenedCollection);

		ChangeStreamIterable<Document> watch = collection.watch(List.of(
				Aggregates.match(
					expr(eq(mod(divide(toLong(toDate())), partitionNumbers), partition))
				)
			))
			.fullDocument(FullDocument.UPDATE_LOOKUP);

		if (resumeToken != null) {
			log.info("Resuming change stream for partition {} on collection {} with token: {}", partition, listenedCollection, resumeToken);
			watch.resumeAfter(new BsonDocument("_data", new BsonString(resumeToken)));
		} else {
			log.info("No resume token found for partition {} on collection {}, starting fresh", partition, listenedCollection);
		}

		do {
			try (var cursor = watch.cursor()) {
				ChangeStreamDocument<Document> document = cursor.tryNext();
				if (document != null) {
					switch (document.getOperationType()) {
						case UPDATE ->
							log.info("UPDATE changedFields: " + document.getUpdateDescription().getUpdatedFields().toJson());
						default ->
							log.info("{} document: {}", document.getOperationType().name(), document.getFullDocument().toJson());
					}

					listeners.forEach(listener -> listener.handle(document));
				} else {
					log.trace("No new updates found on partition {} for collection {}", partition, listenedCollection);
				}

				// Read resumeToken even with no new results to make sure the token does not expire
				BsonDocument resumeTokenDoc = cursor.getResumeToken();
				resumeToken = resumeTokenDoc.getString("_data").getValue();
				log.info("Updating resume token for partition " + partition + ", resumeToken: " + resumeToken);
				configManager.updateResumeToken(configId, resumeToken);
				watch.resumeAfter(resumeTokenDoc);
			} catch (Exception ex) {
				log.error("Exception while processing change", ex);
			}
		} while (true);
	}

	void register(ChangeStreamListener listener) {
		log.info("Registering listener {} to worker on partition {} for collection {}", listener.getClass().getName(), partition, listenedCollection);
		listeners.add(listener);
	}
}
