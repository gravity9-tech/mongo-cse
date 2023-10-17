package com.gravity9.mongocdc.app;

import com.gravity9.mongocdc.MongoCDCManager;

public class Main {

	private static final String URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority";

	private static final String DB_NAME = "test";

	private static final String COLL_NAME = "change_stream_test";

	public static void main(String[] args) {
		int partitionNumbers = 3;
		MongoCDCManager manager = new MongoCDCManager(URI, DB_NAME, COLL_NAME, partitionNumbers);
		manager.start();
	}
}
