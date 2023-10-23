package com.gravity9.mongocdc.constants;

public class TestConstants {

	public static final String CONN_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority";

	public static final String DB_NAME = "test";

	public static final String COLL_NAME = "change_stream_tests";
}
