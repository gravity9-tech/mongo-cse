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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
    private static final Logger log = LoggerFactory.getLogger(MongoChangeStreamWorker.class);
    private static final long DEFAULT_INIT_TIMEOUT_MS = 30 * 1000L;

    private final MongoConfig mongoConfig;
    private final int partition;

    private final ConfigManager configManager;
    private final Set<ChangeStreamListener> listeners;
    private Thread thread = null;
    private String resumeToken;
    private ObjectId configId;
    private CountDownLatch initializationLatch;

    MongoChangeStreamWorker(MongoConfig mongoConfig,
                            ConfigManager configManager,
                            int partition) {
        this.mongoConfig = mongoConfig;
        this.configManager = configManager;
        this.partition = partition;
        this.listeners = new HashSet<>();
        this.initializationLatch = new CountDownLatch(1);
    }

    public void start() {
        log.info("Starting worker for partition {} on collection '{}'", partition, mongoConfig.getCollectionName());
        ChangeStreamWorkerConfig changeStreamWorkerConfig = configManager.getConfigOrInit(mongoConfig.getCollectionName(), partition);
        this.resumeToken = changeStreamWorkerConfig.getResumeToken();
        this.configId = changeStreamWorkerConfig.getId();

        this.thread = new Thread(this);
        this.thread.start();
        log.info("Worker for partition {} on collection {} is now started!", partition, mongoConfig.getCollectionName());
    }

    public void stop() {
        this.thread.stop();
        this.thread = null;
        log.info("Worker for partition {} on collection {} stopped!", partition, mongoConfig.getCollectionName());
    }

    public void awaitInitialization() {
        try {
            initializationLatch.await(DEFAULT_INIT_TIMEOUT_MS, MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
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
            watch.resumeAfter(new BsonDocument("_data", new BsonString(resumeToken)));
        } else {
            log.info("No resume token found for partition {} on collection {}, starting fresh", partition, mongoConfig.getCollectionName());
        }

        boolean firstCursorOpen = true;
        do {
            try (var cursor = watch.cursor()) {
                if (firstCursorOpen) {
                    firstCursorOpen = false;
                    initializationLatch.countDown();
                }

                ChangeStreamDocument<Document> document = cursor.tryNext();
                if (document != null) {
                    Optional<String> changedDocumentIdOpt = getChangedDocumentId(document).map(ObjectId::toHexString);
                    String changedDocumentId = changedDocumentIdOpt.orElse("?");

                    switch (document.getOperationType()) {
                        case UPDATE:
                            if (canLogSensitiveData()) {
                                log.debug("UPDATE, document id: {}, changedFields: {}", changedDocumentId, document.getUpdateDescription() == null ? NULL_STRING : toJson(document.getUpdateDescription().getUpdatedFields()));
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
                if (resumeTokenDoc != null
                        && resumeTokenDoc.containsKey("_data")
                        && resumeTokenDoc.getString("_data").getValue() != null
                ) {
                    resumeToken = resumeTokenDoc.getString("_data").getValue();
                    log.info("Updating resume token for partition " + partition + ", resumeToken: " + resumeToken);
                    configManager.updateResumeToken(configId, resumeToken);
                    watch.resumeAfter(resumeTokenDoc);
                }
            } catch (Exception ex) {
                log.error("Exception while processing change", ex);
            }
        } while (true);
    }

    void register(ChangeStreamListener listener) {
        log.info("Registering listener {} to worker on partition {} for collection {}", listener.getClass().getName(), partition, mongoConfig.getCollectionName());
        listeners.add(listener);
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

}
