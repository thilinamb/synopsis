package synopsis.client.persistence;

import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * An implementation of PersistenceCompletionCallback that saves the deployment
 * configuration of a given Synopsis cluster as a JSON config.
 * It stores the computations and their locations.
 * The current prefix tree is also serialized into the disk.
 *
 * @author Thilina Buddhika
 */
public class BinaryConfigPersistenceCallback implements PersistenceCompletionCallback {

    private final Logger logger = Logger.getLogger(BinaryConfigPersistenceCallback.class);
    private volatile boolean isCompleted = false;

    @Override
    public void handlePersistenceCompletion(OutstandingPersistenceTask task) {
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            fos = new FileOutputStream(new File("/tmp/" + task.getPersistenceTaskId() + ".pstat"));
            dos = new DataOutputStream(fos);
            task.serialize(dos);
            // flush the buffers
            dos.flush();
            fos.flush();
        } catch (IOException e) {
            logger.error("Error dumping the serialized state to disk.", e);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                if (dos != null) {
                    dos.close();
                }
            } catch (IOException e) {
                logger.error("Error closing output streams.", e);
            }
            isCompleted = true;
        }
    }

    @Override
    public boolean isCompleted() {
        return isCompleted;
    }

}

