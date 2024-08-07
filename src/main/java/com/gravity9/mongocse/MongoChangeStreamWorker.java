package com.gravity9.mongocse;

import com.gravity9.mongocse.listener.ChangeStreamListener;
import com.gravity9.mongocse.logging.LoggingUtil;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;


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
    private final MongoClientProvider clientProvider;
    private final CopyOnWriteArraySet<ChangeStreamListener> listeners;
    private final CountDownLatch initializationLatch;
    private final CountDownLatch shutdownLatch;
    private final String workerId;
    private String resumeToken;
    private ObjectId configId;
    private volatile boolean isReadingFromChangeStream = false;

    MongoChangeStreamWorker(MongoConfig mongoConfig,
                            ConfigManager configManager,
                            int partition,
                            String managerId,
                            MongoClientProvider clientProvider) {
        this.mongoConfig = mongoConfig;
        this.configManager = configManager;
        this.partition = partition;
        this.listeners = new CopyOnWriteArraySet<>();
        this.initializationLatch = new CountDownLatch(1);
        this.shutdownLatch = new CountDownLatch(1);
        this.workerId = LoggingUtil.createWorkerId(managerId, partition);
        this.clientProvider = clientProvider;
    }

    public void stop() {
        this.isReadingFromChangeStream = false;
        this.awaitShutdown();
        log.info("{} - Worker for partition {} on collection {} stopped!", workerId, partition, mongoConfig.getCollectionName());
    }

    public void awaitInitialization() {
        awaitCountDownLatch(initializationLatch, DEFAULT_INIT_TIMEOUT_MS);
    }

    @Override
    public void run() {
        LoggingUtil.setLoggingContext(workerId);
        log.info("Starting worker for partition {} on collection '{}'", partition, mongoConfig.getCollectionName());

        initConfiguration();

        MongoClient mongoClient = clientProvider.getClient();
        MongoDatabase db = mongoClient.getDatabase(mongoConfig.getDatabaseName());
        MongoCollection<Document> collection = db.getCollection(this.mongoConfig.getCollectionName());

        processData(collection);
    }

    private void processData(MongoCollection<Document> collection) {
        ChangeStreamIterable<Document> watch = ChangeStreamIterableFactory.createWatch(mongoConfig, collection, partition);

        resumeIfTokenValid(watch);

        boolean firstCursorOpen = false;
        isReadingFromChangeStream = true;

        do {
            try (var cursor = watch.cursor()) {
                firstCursorOpen = logForFirstCursorOpen(firstCursorOpen);

                do {
                    processSingleDocument(cursor.tryNext());
                    // Read resumeToken even with no new results to make sure the token does not expire
                    updateResumeToken(cursor.getResumeToken(), watch);
                } while (isReadingFromChangeStream);
            } catch (Exception ex) {
                log.error("Exception while processing change", ex);
            }
        } while (isReadingFromChangeStream);

        shutdownLatch.countDown();
        LoggingUtil.removeLoggingContext();
    }

    private void processSingleDocument(ChangeStreamDocument<Document> document) {
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
    }

    private void updateResumeToken(BsonDocument resumeTokenDoc, ChangeStreamIterable<Document> watch) {
        if (resumeTokenDoc != null) {
            readResumeToken(resumeTokenDoc).ifPresent(token -> {
                resumeToken = token;
                log.trace("Updating resume token for partition {}, resumeToken: {}", partition, resumeToken);
                configManager.updateResumeToken(configId, resumeToken);
                watch.resumeAfter(resumeTokenDoc);
            });
        }
    }

    void register(ChangeStreamListener listener) {
        log.info("{} - Registering listener {} to worker on partition {} for collection {}", workerId, listener, partition, mongoConfig.getCollectionName());
        listeners.add(listener);
    }

    public void deregister(ChangeStreamListener listener) {
        if (!hasRegisteredListener(listener)) {
            log.warn("{} - Listener {} is not registered for partition {}", workerId, listener, partition);
            return;
        }
        log.info("{} - Unregistering listener {} from worker on partition {} for collection {}", workerId, listener, partition, mongoConfig.getCollectionName());
        listeners.remove(listener);
    }

    public boolean hasRegisteredListener(ChangeStreamListener listener) {
        return listeners.contains(listener);
    }

    private static boolean canLogSensitiveData() {
        return log.isDebugEnabled();
    }

    private void resumeIfTokenValid(ChangeStreamIterable<Document> watch) {
        if (resumeToken == null) {
            log.info("No resume token found for partition {} on collection {}, starting fresh", partition, mongoConfig.getCollectionName());
            return;
        }

        try {
            log.info("Resuming change stream for partition {} on collection {} with token: {}", partition, mongoConfig.getCollectionName(), resumeToken);
            watch.resumeAfter(buildResumeToken(resumeToken));
        } catch (MongoCommandException e) {
            log.warn("Error while resuming with a saved token! Likely, token has expired from the opLog. Resuming fresh... Token: {}", resumeToken, e);
        }
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

    private Optional<String> readResumeToken(BsonDocument resumeTokenDoc) {
        if (resumeTokenDoc.containsKey(RESUME_TOKEN_DATA_PROPERTY)) {
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

    private boolean logForFirstCursorOpen(boolean firstCursorOpen) {
        if (!firstCursorOpen) {
            initializationLatch.countDown();
            log.info("Worker for partition {} on collection {} is now started!", partition, mongoConfig.getCollectionName());
        }

        return true;
    }
}
