package com.gravity9.mongocdc;

import com.gravity9.mongocdc.listener.ChangeStreamListener;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import static com.gravity9.mongocdc.MongoExpressions.divide;
import static com.gravity9.mongocdc.MongoExpressions.eq;
import static com.gravity9.mongocdc.MongoExpressions.expr;
import static com.gravity9.mongocdc.MongoExpressions.mod;
import static com.gravity9.mongocdc.MongoExpressions.or;
import static com.gravity9.mongocdc.MongoExpressions.toDateDocumentKey;
import static com.gravity9.mongocdc.MongoExpressions.toDateFullDocumentId;
import static com.gravity9.mongocdc.MongoExpressions.toLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


class MongoChangeStreamWorker implements Runnable {

    private static final String NULL_STRING = "null";
    private static final String RESUME_TOKEN_DATA_PROPERTY = "_data";
    private static final long DEFAULT_INIT_TIMEOUT_MS = 30 * 1000L;
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 30 * 1000L;
    private static final Logger log = LoggerFactory.getLogger(MongoChangeStreamWorker.class);

    private final MongoConfig mongoConfig;
    private final int partition;

    private final ConfigManager configManager;
    private final CopyOnWriteArraySet<ChangeStreamListener> listeners;
    private final CountDownLatch initializationLatch;
    private final CountDownLatch shutdownLatch;
    private String resumeToken;
    private ObjectId configId;
    private volatile boolean isReadingFromChangeStream = false;

    MongoChangeStreamWorker(MongoConfig mongoConfig,
                            ConfigManager configManager,
                            int partition) {
        this.mongoConfig = mongoConfig;
        this.configManager = configManager;
        this.partition = partition;
        this.listeners = new CopyOnWriteArraySet<>();
        this.initializationLatch = new CountDownLatch(1);
        this.shutdownLatch = new CountDownLatch(1);
    }

    public void stop() {
        this.isReadingFromChangeStream = false;
        this.awaitShutdown();
        log.info("Worker for partition {} on collection {} stopped!", partition, mongoConfig.getCollectionName());
    }

    public void awaitInitialization() {
        awaitCountDownLatch(initializationLatch, DEFAULT_INIT_TIMEOUT_MS);
    }

    @Override
    public void run() {
        log.info("Starting worker for partition {} on collection '{}'", partition, mongoConfig.getCollectionName());

        initConfiguration();

        MongoClient mongoClient = MongoClientProvider.createClient(mongoConfig.getConnectionUri());
        MongoDatabase db = mongoClient.getDatabase(mongoConfig.getDatabaseName());
        MongoCollection<Document> collection = db.getCollection(this.mongoConfig.getCollectionName());

        ChangeStreamIterable<Document> watch = collection.watch(List.of(
                Aggregates.match(
                        or(List.of(
                                partitionMatchExpression(toDateFullDocumentId(), mongoConfig.getNumberOfPartitions(), partition),
                                partitionMatchExpression(toDateDocumentKey(), mongoConfig.getNumberOfPartitions(), partition)
                        ))
                )
            ))
            .fullDocument(mongoConfig.getFullDocument())
            .fullDocumentBeforeChange(mongoConfig.getFullDocumentBeforeChange())
            .maxAwaitTime(mongoConfig.getMaxAwaitTimeInMs(), MILLISECONDS);

        if (resumeToken != null) {
            log.info("Resuming change stream for partition {} on collection {} with token: {}", partition, mongoConfig.getCollectionName(), resumeToken);
            watch.resumeAfter(buildResumeToken(resumeToken));
        } else {
            log.info("No resume token found for partition {} on collection {}, starting fresh", partition, mongoConfig.getCollectionName());
        }

        boolean firstCursorOpen = true;
        isReadingFromChangeStream = true;
        do {
            try (var cursor = watch.cursor()) {
                if (firstCursorOpen) {
                    firstCursorOpen = false;
                    initializationLatch.countDown();
                    log.info("Worker for partition {} on collection {} is now started!", partition, mongoConfig.getCollectionName());
                }

                ChangeStreamDocument<Document> document = cursor.tryNext();
                if (document != null) {
                    Optional<String> changedDocumentIdOpt = getChangedDocumentId(document).map(ObjectId::toHexString);
                    String changedDocumentId = changedDocumentIdOpt.orElse("?");

                    switch (document.getOperationType()) {
                        case UPDATE:
                            if (canLogSensitiveData()) {
                                var updateDescription = document.getUpdateDescription();
                                log.debug("UPDATE, document id: {}, changedFields: {}", changedDocumentId, updateDescription == null ? NULL_STRING : toJson(updateDescription.getUpdatedFields()));
                            } else {
                                log.info("UPDATE, document id: {}", changedDocumentId);
                            }
                            break;
                        case DELETE:
                            log.info("DELETE, document id: {}", changedDocumentId);
                            break;
                        default:
                            if (canLogSensitiveData()) {
                                log.debug("{} document: {}", document.getOperationType().name(), toJson(document.getFullDocument()));
                            } else {
                                log.info("{} document id: {}", document.getOperationType().name(), changedDocumentId);
                            }
                    }

                    listeners.forEach(listener -> listener.handle(document));
                } else {
                    log.trace("No new updates found on partition {} for collection {}", partition, mongoConfig.getCollectionName());
                }

                // Read resumeToken even with no new results to make sure the token does not expire
                BsonDocument resumeTokenDoc = cursor.getResumeToken();
                var resumeTokenOpt = readResumeToken(resumeTokenDoc);
                if (resumeTokenOpt.isPresent()) {
                    resumeToken = resumeTokenOpt.get();
                    log.info("Updating resume token for partition " + partition + ", resumeToken: " + resumeToken);
                    configManager.updateResumeToken(configId, resumeToken);
                    watch.resumeAfter(resumeTokenDoc);
                }
            } catch (Exception ex) {
                log.error("Exception while processing change", ex);
            } finally {
                if (!isReadingFromChangeStream) {
                    shutdownLatch.countDown();
                }
            }
        } while (isReadingFromChangeStream);
    }

    void register(ChangeStreamListener listener) {
		log.info("Registering listener {} to worker on partition {} for collection {}", listener, partition, mongoConfig.getCollectionName());
		listeners.add(listener);
	}

	public void deregister(ChangeStreamListener listener) {
        if (!hasRegisteredListener(listener)) {
            log.warn("Listener {} is not registered for partition {}", listener, partition);
            return;
        }
		log.info("Unregistering listener {} from worker on partition {} for collection {}", listener, partition, mongoConfig.getCollectionName());
		listeners.remove(listener);
	}

	public boolean hasRegisteredListener(ChangeStreamListener listener) {
		return listeners.contains(listener);
	}

    private static boolean canLogSensitiveData() {
        return log.isDebugEnabled();
    }

    private Optional<ObjectId> getChangedDocumentId(ChangeStreamDocument<Document> document) {
        Document fullDocument = document.getFullDocument();
        if (fullDocument != null && fullDocument.containsKey("_id")) {
            return Optional.ofNullable(fullDocument.getObjectId("_id"));
        }

        BsonDocument documentKey = document.getDocumentKey();
        if (documentKey != null && documentKey.containsKey("_id")) {
            return Optional.ofNullable(documentKey.getObjectId("_id").getValue());
        }

        return Optional.empty();
    }

    private String toJson(BsonDocument document) {
        return document == null ? NULL_STRING : document.toJson();
    }

    private String toJson(Document document) {
        return document == null ? NULL_STRING : document.toJson();
    }

    private Bson partitionMatchExpression(Bson documentId, int partitionNumbers, int partitionNo) {
        return expr(eq(mod(divide(toLong(documentId)), partitionNumbers), partitionNo));
    }

    private Optional<String> readResumeToken(BsonDocument resumeTokenDoc) {
        if (resumeTokenDoc != null
                && resumeTokenDoc.containsKey(RESUME_TOKEN_DATA_PROPERTY)
        ) {
            return Optional.ofNullable(resumeTokenDoc.getString(RESUME_TOKEN_DATA_PROPERTY).getValue());
        }
        return Optional.empty();
    }

    private BsonDocument buildResumeToken(String aResumeToken) {
        return new BsonDocument(RESUME_TOKEN_DATA_PROPERTY, new BsonString(aResumeToken));
    }

    private void awaitShutdown() {
        awaitCountDownLatch(shutdownLatch, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    private void awaitCountDownLatch(CountDownLatch countDownLatch, long timeout) {
        try {
            countDownLatch.await(timeout, MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initConfiguration() {
        ChangeStreamWorkerConfig changeStreamWorkerConfig = configManager.getConfigOrInit(mongoConfig.getCollectionName(), partition);
        this.resumeToken = changeStreamWorkerConfig.getResumeToken();
        this.configId = changeStreamWorkerConfig.getId();
    }
}
