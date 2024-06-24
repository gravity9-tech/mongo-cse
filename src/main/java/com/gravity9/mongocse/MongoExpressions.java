package com.gravity9.mongocse;

import java.util.List;

import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

class MongoExpressions {

	static Bson expr(Bson expr) {
		return new Document("$expr", expr);
	}

	static Bson eq(Bson expr, int value) {
		return new Document("$eq", List.of(expr, value));
	}

	static Bson or(List<Bson> expressions) {
		return new Document("$or", expressions);
	}

	static Bson and(List<Bson> expressions) {
		return new Document("$and", expressions);
	}

	static Bson mod(Bson expr, int value) {
		return new Document("$mod", List.of(expr, value));
	}

	static BsonString fullDocumentKey(String keyName) {
		return new BsonString("$fullDocument." + keyName);
	}

	static BsonString documentKey(String keyName) {
		return new BsonString("$documentKey." + keyName);
	}

	static Bson abs(Bson expr) {
		return new Document("$abs", expr);
	}

	static Bson toHashedIndexKey(BsonValue expr) {
		return new Document("$toHashedIndexKey", expr);
	}

	static Bson cond(Object ifExpr, Object thenExpr, Object elseExpr) {
		return new Document("$cond", List.of(ifExpr, thenExpr, elseExpr));
	}
}
