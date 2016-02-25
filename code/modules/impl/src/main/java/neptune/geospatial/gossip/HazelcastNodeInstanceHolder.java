package neptune.geospatial.gossip;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.log4j.Logger;

/**
 * Holds a singleton instance of a {@code HazelcastInstance}.
 * This is initialized during the Resource start up.
 * The member variable {@code HazelcastInstance} is not thread safe.
 * To guard against any possible concurrency issues, client
 * should call the {@code beginOperation} before using it.
 * After the operation, {@code endOperation} should be called
 * to release the lock.
 *
 * @author Thilina Buddhika
 */
public class HazelcastNodeInstanceHolder extends SerialAccessSingleton{

    private static HazelcastNodeInstanceHolder instance;

    private HazelcastInstance hazelcastInstance;
    private static Logger logger = Logger.getLogger(HazelcastNodeInstanceHolder.class);

    private HazelcastNodeInstanceHolder(Config config) {
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    /**
     * Initialize the singleton {@code HazelcastNodeInstanceHolder}
     * @param config Hazelcast Config
     */
    public static synchronized void init(Config config) {
        if (instance == null) {
            instance = new HazelcastNodeInstanceHolder(config);
        }
        logger.info("Successfully started the Hazelcast node. Cluster Size: " +
                instance.hazelcastInstance.getCluster().getMembers().size());
    }

    /**
     * Retrieves the singleton {@code HazelcastInstanceHolder}
     *
     * @return {@code HazelcastInstanceHolder} object
     * @throws HazelcastException {@code HazelcastInstanceHolder} object is not initialized yet.
     */
    public static synchronized HazelcastNodeInstanceHolder getInstance() throws HazelcastException {
        if (instance == null) {
            throw new HazelcastException("Hazelcast is not initialized yet.");
        }
        return instance;
    }

}
