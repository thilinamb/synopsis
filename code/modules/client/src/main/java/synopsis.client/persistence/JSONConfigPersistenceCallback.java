package synopsis.client.persistence;

import com.google.gson.Gson;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;
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
public class JSONConfigPersistenceCallback implements PersistenceCompletionCallback {

    private final Logger logger = Logger.getLogger(JSONConfigPersistenceCallback.class);

    @Override
    public void handlePersistenceCompletion(OutstandingPersistenceTask task) {
        Gson gson = new Gson();
        byte[] bytes = gson.toJson(task).getBytes();
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            fos = new FileOutputStream(new File("/tmp/" + task.getPersistenceTaskId() + ".pstat"));
            dos = new DataOutputStream(fos);
            dos.writeInt(bytes.length);
            dos.write(bytes);
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
        }
    }

    public static void main(String[] args) {
        OutstandingPersistenceTask task = new OutstandingPersistenceTask(1000l, 1);
        task.handlePersistStateResp(new PersistStateResponse(1000l, "/tmp/", "comp-1", new byte[1000]));
        Gson gson = new Gson();
        String jsonStr = gson.toJson(task);
        OutstandingPersistenceTask task2 = gson.fromJson(jsonStr, OutstandingPersistenceTask.class);
        System.out.println("Done!");
    }
}

