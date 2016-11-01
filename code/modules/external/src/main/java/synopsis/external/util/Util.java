package synopsis.external.util;

import synopsis.client.persistence.OutstandingPersistenceTask;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class Util {
    public static OutstandingPersistenceTask deserializeOutstandingPersistenceTask(String serializedDeplomentPlanLoc) {
        FileInputStream fis = null;
        DataInputStream dis = null;
        OutstandingPersistenceTask outstandingPersistenceTask = null;
        try {
            fis = new FileInputStream(serializedDeplomentPlanLoc);
            dis = new DataInputStream(fis);
            outstandingPersistenceTask = new OutstandingPersistenceTask();
            outstandingPersistenceTask.deserialize(dis);
            System.out.println("Successfully deserliazed the OutstandingPersistenceTask!");
        } catch (IOException e) {
            System.err.println("Error reading the deployment plan.");
            e.printStackTrace();
            System.exit(-1);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
                if (dis != null) {
                    dis.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing file streams.");
                e.printStackTrace();
            }
        }
        return outstandingPersistenceTask;
    }
}
