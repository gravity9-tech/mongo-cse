package com.gravity9.mongocse;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.gravity9.mongocse.MongoExpressions.abs;
import static com.gravity9.mongocse.MongoExpressions.and;
import static com.gravity9.mongocse.MongoExpressions.cond;
import static com.gravity9.mongocse.MongoExpressions.documentKey;
import static com.gravity9.mongocse.MongoExpressions.eq;
import static com.gravity9.mongocse.MongoExpressions.expr;
import static com.gravity9.mongocse.MongoExpressions.fullDocumentBeforeChangeKey;
import static com.gravity9.mongocse.MongoExpressions.fullDocumentKey;
import static com.gravity9.mongocse.MongoExpressions.mod;
import static com.gravity9.mongocse.MongoExpressions.toHashedIndexKey;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ChangeStreamIterableFactory {

    private static final int DEFAULT_PARTITION = 0;

    static ChangeStreamIterable<Document> createWatch(MongoConfig mongoConfig, MongoCollection<Document> collection, int partition) {
        return collection.watch(List.of(
                        Aggregates.match(
                          and(List.of(
                            mongoConfig.getMatch(),
                            partitionMatchExpression(mongoConfig.getKeyName(), mongoConfig.getNumberOfPartitions(), partition)
                          ))
                        )
                ))
                .fullDocument(mongoConfig.getFullDocument())
                .fullDocumentBeforeChange(mongoConfig.getFullDocumentBeforeChange())
                .maxAwaitTime(mongoConfig.getMaxAwaitTimeInMs(), MILLISECONDS);
    }

    private static Bson partitionMatchExpression(String keyName, int partitionNumbers, int partitionNo) {
        BsonValue fullDocKey = fullDocumentKey(keyName);
        BsonValue docKey = documentKey(keyName);
        BsonValue beforeChangeKey = fullDocumentBeforeChangeKey(keyName);

        return expr(eq(
            cond(fullDocKey, mod(abs(toHashedIndexKey(fullDocKey)), partitionNumbers),
                cond(docKey, mod(abs(toHashedIndexKey(docKey)), partitionNumbers),
                    cond(beforeChangeKey, mod(abs(toHashedIndexKey(beforeChangeKey)), partitionNumbers),
                        DEFAULT_PARTITION))),
            partitionNo));
    }

}
