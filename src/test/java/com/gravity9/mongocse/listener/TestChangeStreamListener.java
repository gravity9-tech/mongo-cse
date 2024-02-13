package com.gravity9.mongocse.listener;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.bson.Document;

public class TestChangeStreamListener implements ChangeStreamListener {

	List<ChangeStreamDocument<Document>> events = Collections.synchronizedList(new ArrayList<>());

	@Override
	public void handle(ChangeStreamDocument<Document> event) {
		events.add(event);
	}

	public List<ChangeStreamDocument<Document>> getEvents() {
		return events;
	}
}
