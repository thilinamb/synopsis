package neptune.geospatial.core.computations.scalingctxt;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import ds.granules.exception.GranulesConfigurationException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.hazelcast.type.SketchLocation;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Maintains the state required for dynamic scaling.
 * State includes prefix statistics {@code MonitoredPrefix}, pending scale out requests
 * {@code PendingScaleOutRequest} and pending scale in requests {@code PendingScaleInRequest}.
 * <p/>
 * Each {@code AbstractGeoSpatialStreamProcess} object has an instance of ScalingContext which
 * is updated with each stream message as well as protocol messages for dynamic scaling.
 *
 * @author Thilina Buddhika
 */
public class ScalingContext {

    private final Logger logger = Logger.getLogger(ScalingContext.class);

    private final Set<MonitoredPrefix> monitoredPrefixes = new TreeSet<>();
    private final Map<String, MonitoredPrefix> monitoredPrefixMap = new HashMap<>();
    private final Map<String, PendingScaleOutRequest> pendingScaleOutRequests = new HashMap<>();
    private final Map<String, PendingScaleInRequest> pendingScaleInRequests = new HashMap<>();
    private HazelcastInstance hzInstance;
    private final String instanceIdentifier;

    /**
     * @param instanceIdentifier Instance Identifier of the underlying computation
     */
    public ScalingContext(String instanceIdentifier) {
        this.instanceIdentifier = instanceIdentifier;
    }

    /**
     * Returns a monitored prefix
     *
     * @param prefix Prefix
     * @return {@code MonitoredPrefix} instance or null if the prefix is not being monitored
     */
    public MonitoredPrefix getMonitoredPrefix(String prefix) {
        return monitoredPrefixMap.get(prefix);
    }

    /**
     * Register a new monitored prefix
     * @param prefix Prefix string
     * @param monitoredPrefix {@code MonitoredPrefix} object
     */
    public void addMonitoredPrefix(String prefix, MonitoredPrefix monitoredPrefix){
        monitoredPrefixMap.put(prefix, monitoredPrefix);
        monitoredPrefixes.add(monitoredPrefix);
    }

    /**
     * increments the message counts for a monitored prefix
     *
     * @param prefix    prefix
     * @param className Type of the record
     */
    public void updateMessageCount(String prefix, String className) {
        MonitoredPrefix monitoredPrefix;
        if (monitoredPrefixMap.containsKey(prefix)) {
            monitoredPrefix = monitoredPrefixMap.get(prefix);
        } else {
            monitoredPrefix = new MonitoredPrefix(prefix, className);
            monitoredPrefixes.add(monitoredPrefix);
            monitoredPrefixMap.put(prefix, monitoredPrefix);
            try {
                IMap<String, SketchLocation> prefMap = getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                prefMap.put(prefix, new SketchLocation(instanceIdentifier, RivuletUtil.getCtrlEndpoint(),
                        SketchLocation.MODE_REGISTER_NEW_PREFIX));
            } catch (HazelcastException e) {
                logger.error("Error retrieving HzInstance.", e);
            } catch (GranulesConfigurationException e) {
                logger.error("Error retrieving the ctrl endpoint.", e);
            }
        }
        monitoredPrefix.incrementMessageCount();
    }

    public void updateMessageRates(double timeElapsed) {
        for (String monitoredPrefStr : monitoredPrefixMap.keySet()) {
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(monitoredPrefStr);
            monitoredPrefix.updateMessageRate(timeElapsed);
            if (logger.isTraceEnabled()) {
                logger.trace(String.format("[%s] Prefix: %s, Message Rate: %.3f", instanceIdentifier,
                        monitoredPrefStr, monitoredPrefix.getMessageRate()));
            }
        }
    }

    public List<String> getPrefixesForScalingOut(Double excess) {
        List<String> prefixesForScalingOut = new ArrayList<>();
        double cumulSumOfPrefixes = 0;
        Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
        while (itr.hasNext() && cumulSumOfPrefixes < excess) {
            MonitoredPrefix monitoredPrefix = itr.next();
            if (!monitoredPrefix.getIsPassThroughTraffic() && monitoredPrefix.getMessageRate() > 0 &&
                    monitoredPrefix.getPrefix().length() < AbstractGeoSpatialStreamProcessor.MAX_CHARACTER_DEPTH) {
                prefixesForScalingOut.add(monitoredPrefix.getPrefix());
                // let's consider the number of messages accumulated over 2s.
                cumulSumOfPrefixes += monitoredPrefix.getMessageRate() * 2;
            }
        }
        if (logger.isDebugEnabled()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String prefix : prefixesForScalingOut) {
                stringBuilder.append(prefix).append("(").append(monitoredPrefixMap.get(prefix).getMessageRate()).
                        append("), ");
            }
            logger.debug(String.format("[%s] Scale Out recommendation. Excess: %.3f, Chosen Prefixes: %s",
                    instanceIdentifier, excess, stringBuilder.toString()));
        }
        return prefixesForScalingOut;
    }

    public List<String> getPrefixesForScalingIn(Double excess) {
        // find the prefixes with the lowest input rates that are pass-through traffic
        Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
        List<String> chosenPrefixes = new ArrayList<>();
        while (itr.hasNext()) {
            MonitoredPrefix monitoredPrefix = itr.next();
            if (monitoredPrefix.getIsPassThroughTraffic()) {
                chosenPrefixes.add(monitoredPrefix.getPrefix());
                // FIXME: Scale in just one computation, just to make sure protocol works
                break;
            }
        }
        return chosenPrefixes;
    }

    /**
     * Returns the set of child prefixes for propagating a scale in request
     *
     * @param prefix Parent prefix
     * @return List of child prefixes
     */
    public List<MonitoredPrefix> getChildPrefixesForScalingIn(String prefix) {
        List<MonitoredPrefix> childPrefixes = new ArrayList<>();
        for (String monitoredPrefix : monitoredPrefixMap.keySet()) {
            if (monitoredPrefix.startsWith(prefix)) {
                childPrefixes.add(monitoredPrefixMap.get(monitoredPrefix));
            }
        }
        return childPrefixes;
    }

    public void addPendingScaleOutRequest(String key, PendingScaleOutRequest pendingScaleOutRequest) {
        pendingScaleOutRequests.put(key, pendingScaleOutRequest);
    }

    public PendingScaleOutRequest getPendingScaleOutRequest(String key) {
        return pendingScaleOutRequests.get(key);
    }

    public void completeScalingOut(String key) {
        pendingScaleOutRequests.remove(key);
    }

    public void addPendingScalingInRequest(String key, PendingScaleInRequest scaleInRequest) {
        pendingScaleInRequests.put(key, scaleInRequest);
    }

    public PendingScaleInRequest getPendingScalingInRequest(String key){
        return pendingScaleInRequests.get(key);
    }

    public void removePendingScaleInRequest(String key){
        pendingScaleInRequests.remove(key);
    }

    private HazelcastInstance getHzInstance() throws HazelcastException {
        if (hzInstance == null) {
            synchronized (this) {
                if (hzInstance == null) {
                    try {
                        hzInstance = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance();
                    } catch (HazelcastException e) {
                        throw e;
                    }
                }
            }
        }
        return hzInstance;
    }
}
