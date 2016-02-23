package neptune.geospatial.util;

import java.util.concurrent.Semaphore;

/**
 * Wrapping the {@code Semaphore} to implement a mutex.
 *
 * @author Thilina Buddhika
 */
public class Mutex {

    private final Semaphore semaphore;

    public Mutex() {
        semaphore = new Semaphore(1);
    }

    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    public void release() {
        if (semaphore.availablePermits() == 0) {
            semaphore.release();
        }
    }
}
