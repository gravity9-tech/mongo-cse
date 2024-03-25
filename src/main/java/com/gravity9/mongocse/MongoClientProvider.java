package com.gravity9.mongocse;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.io.Closeable;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

class MongoClientProvider implements Closeable {

	private final String mongoConnectionUri;

	private MongoClient client;

    MongoClientProvider(String mongoConnectionUri) {
        this.mongoConnectionUri = mongoConnectionUri;
    }

    public MongoClient getClient() {
		if (client == null) {
			client = createClient();
		}

		return client;
	}

	public MongoClient createClient() {
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

	@Override
	public void close() {
		if (client != null) {
			client.close();
			client = null;
		}
	}
}
