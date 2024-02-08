package com.gravity9.mongocdc;

import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

public class MongoConfig {

	private final String connectionUri;

	private final String databaseName;

	private final String collectionName;

	private final String keyName;

	private final int numberOfPartitions;

	private final String workerConfigCollectionName;

	private final String clusterConfigCollectionName;

	private final FullDocument fullDocument;

	private final FullDocumentBeforeChange fullDocumentBeforeChange;

	private final long maxAwaitTimeInMs;

	public MongoConfig(MongoConfigBuilder mongoConfigBuilder) {
		this.connectionUri = mongoConfigBuilder.connectionUri;
		this.databaseName = mongoConfigBuilder.databaseName;
		this.collectionName = mongoConfigBuilder.collectionName;
		this.keyName = mongoConfigBuilder.keyName;
		this.numberOfPartitions = mongoConfigBuilder.numberOfPartitions;
		this.workerConfigCollectionName = mongoConfigBuilder.workerConfigCollectionName;
		this.clusterConfigCollectionName = mongoConfigBuilder.clusterConfigCollectionName;
		this.fullDocument = mongoConfigBuilder.fullDocument;
		this.fullDocumentBeforeChange = mongoConfigBuilder.fullDocumentBeforeChange;
		this.maxAwaitTimeInMs = mongoConfigBuilder.maxAwaitTimeInMs;
	}

	public static MongoConfigBuilder builder() {
		return new MongoConfigBuilder();
	}

	public String getConnectionUri() {
		return connectionUri;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public String getKeyName() {
		return keyName;
	}

	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}

	public String getWorkerConfigCollectionName() {
		return workerConfigCollectionName;
	}

	public String getClusterConfigCollectionName() {
		return clusterConfigCollectionName;
	}

	public FullDocument getFullDocument() {
		return fullDocument;
	}

	public FullDocumentBeforeChange getFullDocumentBeforeChange() {
		return fullDocumentBeforeChange;
	}

	public long getMaxAwaitTimeInMs() {
		return maxAwaitTimeInMs;
	}

	public static class MongoConfigBuilder {

		private String connectionUri;

		private String databaseName;

		private String collectionName;

		private String keyName = "_id";

		private int numberOfPartitions;

		private String workerConfigCollectionName = "changeStreamWorkerConfig";

		private String clusterConfigCollectionName = "changeStreamClusterConfig";

		private FullDocument fullDocument = FullDocument.UPDATE_LOOKUP;

		private FullDocumentBeforeChange fullDocumentBeforeChange = FullDocumentBeforeChange.DEFAULT;

		@SuppressWarnings("checkstyle:magicnumber")
		private long maxAwaitTimeInMs = 1000;

		public MongoConfigBuilder connectionUri(String connectionUri) {
			this.connectionUri = connectionUri;
			return this;
		}

		public MongoConfigBuilder databaseName(String databaseName) {
			this.databaseName = databaseName;
			return this;
		}

		public MongoConfigBuilder collectionName(String collectionName) {
			this.collectionName = collectionName;
			return this;
		}

		public MongoConfigBuilder keyName(String keyName) {
			this.keyName = keyName;
			return this;
		}

		public MongoConfigBuilder numberOfPartitions(int numberOfPartitions) {
			this.numberOfPartitions = numberOfPartitions;
			return this;
		}

		public MongoConfigBuilder workerConfigCollectionName(String workerConfigCollectionName) {
			this.workerConfigCollectionName = workerConfigCollectionName;
			return this;
		}

		public MongoConfigBuilder clusterConfigCollectionName(String clusterConfigCollectionName) {
			this.clusterConfigCollectionName = clusterConfigCollectionName;
			return this;
		}

		public MongoConfigBuilder fullDocument(FullDocument fullDocument) {
			this.fullDocument = fullDocument;
			return this;
		}

		public MongoConfigBuilder fullDocumentBeforeChange(FullDocumentBeforeChange fullDocumentBeforeChange) {
			this.fullDocumentBeforeChange = fullDocumentBeforeChange;
			return this;
		}

		public MongoConfigBuilder maxAwaitTimeInMs(long maxAwaitTimeInMs) {
			this.maxAwaitTimeInMs = maxAwaitTimeInMs;
			return this;
		}
		public MongoConfig build() {
			return new MongoConfig(this);
		}
	}

}
