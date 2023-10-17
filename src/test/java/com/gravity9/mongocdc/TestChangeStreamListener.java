package com.gravity9.mongocdc;

import com.gravity9.mongocdc.listener.ChangeStreamListener;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

public class TestChangeStreamListener implements ChangeStreamListener {

	List<ChangeStreamDocument<Document>> events = new ArrayList<>();

	@Override
	public void handle(ChangeStreamDocument<Document> event) {
		events.add(event);
	}

	public List<ChangeStreamDocument<Document>> getEvents() {
		return events;
	}
}
