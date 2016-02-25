package neptune.geospatial.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.apache.log4j.Logger;

/**
 * A singleton instance holder for {@code HazelcastClientInstanceHolder}
 *
 * The member variable {@code HazelcastInstance} is not thread safe.
 * To guard against any possible concurrency issues, client
 * should call the {@code beginOperation} before using it.
 * After the operation, {@code endOperation} should be called
 * to release the lock.
 *
 * @author Thilina Buddhika
 */
public class HazelcastClientInstanceHolder extends SerialAccessSingleton {

    private static Logger logger = Logger.getLogger(HazelcastNodeInstanceHolder.class);
    private static HazelcastClientInstanceHolder instance;

    private HazelcastInstance hazelcastClientInstance;

    private HazelcastClientInstanceHolder(ClientConfig config) {
        while (true) {
            try {
                hazelcastClientInstance = HazelcastClient.newHazelcastClient(config);
                break;
            } catch (Exception e) {
                logger.info("No servers present!. Waiting until a server becomes available.");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignore) {

                }
            }
        }
    }

    /**
     * Initialize the singleton {@code HazelcastClientInstanceHolder}
     *
     * @param config Hazelcast Client Config
     */
    public static synchronized void init(ClientConfig config) {
        if (instance == null) {
            instance = new HazelcastClientInstanceHolder(config);
        }
        logger.info("Successfully started the Hazelcast client. Cluster Size: " +
                instance.hazelcastClientInstance.getCluster().getMembers().size());
    }

    /**
     * Retrieves the singleton {@code HazelcastClientInstanceHolder}
     *
     * @return {@code HazelcastClientInstanceHolder} object
     * @throws HazelcastException {@code HazelcastClientInstanceHolder} object is not initialized yet.
     */
    public static synchronized HazelcastClientInstanceHolder getInstance() throws HazelcastException {
        if (instance == null) {
            throw new HazelcastException("Hazelcast is not initialized yet.");
        }
        return instance;
    }
}
