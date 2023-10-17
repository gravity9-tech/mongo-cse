package com.gravity9.mongocdc;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

class MongoClientProvider {

	static MongoClient createClient(String mongoConnectionUri) {
		ConnectionString connectionString = new ConnectionString(mongoConnectionUri);
		CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
		CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
			pojoCodecRegistry);
		MongoClientSettings clientSettings = MongoClientSettings.builder()
			.applyConnectionString(connectionString)
			.codecRegistry(codecRegistry)
			.build();

		return MongoClients.create(clientSettings);
	}
}
