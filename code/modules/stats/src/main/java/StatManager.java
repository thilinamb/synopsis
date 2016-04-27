import neptune.geospatial.stat.InstanceRegistration;
import neptune.geospatial.stat.PeriodicInstanceMetrics;
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

    private Map<String, double[]> registry = new HashMap<>();

    private StatManager() {
    }

    static StatManager getInstance() {
        return instance;
    }

    synchronized void register(InstanceRegistration registerMessage) {
        String instanceId = registerMessage.getInstanceId();
        if (!registry.containsKey(instanceId)) {
            registry.put(instanceId, new double[]{-1.0, -1.0, -1.0, -1.0, -1.0});
            logger.info(String.format("Registered new instance. Instance Id: %s, Endpoint: %s",
                    instanceId, registerMessage.getOriginEndpoint()));
        } else {
            logger.warn(String.format("Duplicate registration message. Instance id: %s, Endpoint: %s",
                    instanceId, registerMessage.getOriginEndpoint()));
        }
    }

    synchronized void updateMetrics(PeriodicInstanceMetrics msg) {
        String instanceId = msg.getInstanceId();
        if (registry.containsKey(instanceId)) {
            registry.put(instanceId, msg.getMetrics());
        } else {
            logger.warn(String.format("Invalid metrics update. Instance Id: %s, Endpoint: %s", instanceId,
                    msg.getOriginEndpoint()));
        }
    }

}
