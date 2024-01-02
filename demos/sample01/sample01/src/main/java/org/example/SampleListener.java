package org.example;

import com.gravity9.mongocdc.listener.ChangeStreamListener;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.UUID;

@Slf4j
public class SampleListener implements ChangeStreamListener {

    private final String name;

    public SampleListener() {
        this.name = UUID.randomUUID().toString();
    }

    @Override
    public void handle(ChangeStreamDocument<Document> changeStreamDocument) {
        if (changeStreamDocument.getFullDocument() != null && changeStreamDocument.getFullDocument().getString("description") != null) {
            String description = changeStreamDocument.getFullDocument().getString("description");
            System.out.printf("Listener %s received event with description: %s%n", name, description);
        } else if (changeStreamDocument.getDocumentKey() != null) {
            System.out.printf("Listener %s received event with document key: %s%n", name, changeStreamDocument.getDocumentKey().toJson());
        } else {
            System.out.printf("Listener %s received event: %s%n", name, changeStreamDocument);
        }
    }
}
