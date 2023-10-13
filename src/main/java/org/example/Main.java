package org.example;


public class Main {
    public static void main(String[] args) {
        String uri = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority";
        int partitionNumbers = 3;

//        new Thread(new ModificationThread(uri, "test", "change_stream_test")).start();
        for (int i = 0; i< partitionNumbers; i++) {
            new Thread(new ChangeStreamListener(uri, "test", "change_stream_test",  partitionNumbers, i)).start();
        }
    }
}
