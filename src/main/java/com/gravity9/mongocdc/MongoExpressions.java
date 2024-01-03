package com.gravity9.mongocdc;

import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

class MongoExpressions {

	private static final Integer TO_MILLIS = 1000;

	static Bson expr(Bson expr) {
		return new Document("$expr", expr);
	}

	static Bson eq(Bson expr, int value) {
		return new Document("$eq", List.of(expr, value));
	}
	static Bson or(List<Bson> expressions) {
		return new Document("$or", expressions);
	}

	static Bson mod(Bson expr, int value) {
		return new Document("$mod", List.of(expr, value));
	}

	static Bson divide(Bson expr) {
		return new Document("$divide", List.of(expr, TO_MILLIS));
	}

	static Bson toLong(Bson expr) {
		return new Document("$toLong", expr);
	}

	static Bson toDateFullDocumentId() {
		return new Document("$toDate", "$fullDocument._id");
	}

	static Bson toDateDocumentKey() {
		return new Document("$toDate", "$documentKey._id");
	}
}
