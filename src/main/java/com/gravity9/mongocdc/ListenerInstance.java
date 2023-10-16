package com.gravity9.mongocdc;

import org.bson.types.ObjectId;

public class ListenerInstance {

    private ObjectId id;
    private String role;


    public ListenerInstance(ObjectId id, String role) {
        this.id = id;
        this.role = role;
    }
}
