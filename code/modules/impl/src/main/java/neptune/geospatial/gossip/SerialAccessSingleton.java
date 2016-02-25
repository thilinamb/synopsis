package neptune.geospatial.gossip;

import java.util.concurrent.Semaphore;

/**
 * Provides a mutex to control concurrent access.
 *
 * @author Thilina Buddhika
 */
public class SerialAccessSingleton {

    private static Semaphore lock = new Semaphore(1);

    /**
     * Acquire the lock before doing any operation
     */
    public static void beginOperation() {
        try {
            lock.acquire();
        } catch (InterruptedException ignore) {

        }
    }

    /**
     * Release the lock after using the singleton object
     */
    public static void endOperation() {
        if(lock.availablePermits() == 0) {
            lock.release();
        }
    }
}
