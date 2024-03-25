package com.gravity9.mongocse;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.gravity9.mongocse.MongoExpressions.divide;
import static com.gravity9.mongocse.MongoExpressions.eq;
import static com.gravity9.mongocse.MongoExpressions.expr;
import static com.gravity9.mongocse.MongoExpressions.mod;
import static com.gravity9.mongocse.MongoExpressions.or;
import static com.gravity9.mongocse.MongoExpressions.toDateDocumentKey;
import static com.gravity9.mongocse.MongoExpressions.toDateFullDocumentKey;
import static com.gravity9.mongocse.MongoExpressions.toLong;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ChangeStreamIterableFactory {

    static ChangeStreamIterable<Document> createWatch(MongoConfig config, MongoCollection<Document> collection, int partition) {
        return collection.watch(List.of(
                        Aggregates.match(
                                or(List.of(
                                        partitionMatchExpression(toDateFullDocumentKey(config.getKeyName()), config.getNumberOfPartitions(), partition),
                                        partitionMatchExpression(toDateDocumentKey(config.getKeyName()), config.getNumberOfPartitions(), partition)
                                ))
                        )
                ))
                .fullDocument(config.getFullDocument())
                .fullDocumentBeforeChange(config.getFullDocumentBeforeChange())
                .maxAwaitTime(config.getMaxAwaitTimeInMs(), MILLISECONDS);
    }

    private static Bson partitionMatchExpression(Bson documentId, int partitionNumbers, int partitionNo) {
        return expr(eq(mod(divide(toLong(documentId)), partitionNumbers), partitionNo));
    }
}
