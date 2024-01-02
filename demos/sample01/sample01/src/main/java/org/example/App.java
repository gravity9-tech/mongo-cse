package org.example;

import com.gravity9.mongocdc.MongoCDCManager;
import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 *
 */
@Slf4j
public class App
{
    private static final String MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=dbrs&retryWrites=true&w=majority";
    private static final String DB_NAME = "sample01";
    private static final String COLL_NAME = "sales";

    public static void main(String[] args )
    {
        MongoCDCManager manager = new MongoCDCManager(MONGO_URI, DB_NAME, COLL_NAME, 3);

        var listener1 = new SampleListener();
        manager.registerListenerToAllPartitions(listener1);

        var listener2 = new SampleListener();
        manager.registerListenerToAllPartitions(listener2);

        System.out.println( "Listeners registered, starting manager..." );
        manager.start();

        System.out.println( "press Ctrl+C to quit..." );
        waitForControlC();
        System.out.println("Bye!");
    }

    private static void waitForControlC() {
        try {
            while(true) {
                synchronized (Thread.currentThread()) {
                    Thread.currentThread().wait(); // This is the "work" this class is doing.
                }
                Thread.sleep(1_000L);
            }
        } catch (InterruptedException ignored) {
        }
    }
}
