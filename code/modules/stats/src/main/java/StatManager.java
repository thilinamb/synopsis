import neptune.geospatial.stat.RegistrationMessage;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Process statistics messages and maintains the live state of the entire system
 *
 * @author Thilina Buddhika
 */
class StatManager {

    private static final StatManager instance = new StatManager();
    private final Logger logger = Logger.getLogger(StatManager.class);

    private Map<String, Double[]> registry = new HashMap<>();

    private StatManager() {
    }

    static StatManager getInstance() {
        return instance;
    }

    synchronized void register(RegistrationMessage registerMessage) {
        String instanceId = registerMessage.getInstanceId();
        if (!registry.containsKey(instanceId)) {
            registry.put(instanceId, new Double[]{-1.0, -1.0, -1.0, -1.0, -1.0});
            logger.info(String.format("Registered new instance. Instance Id: %s, Endpoint: %s",
                    instanceId, registerMessage.getOriginEndpoint()));
        } else {
            logger.warn(String.format("Duplicate registration message. Instance id: %s, Endpoint: %s",
                    instanceId));
        }
    }

}
