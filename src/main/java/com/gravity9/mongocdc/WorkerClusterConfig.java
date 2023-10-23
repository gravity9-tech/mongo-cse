package com.gravity9.mongocdc;

import org.bson.types.ObjectId;

public class WorkerClusterConfig {

    private ObjectId id;
    private String collection;
    private int partitions;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }
}
