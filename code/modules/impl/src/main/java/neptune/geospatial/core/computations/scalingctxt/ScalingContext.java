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
     *
     * @param prefix          Prefix string
     * @param monitoredPrefix {@code MonitoredPrefix} object
     */
    public void addMonitoredPrefix(String prefix, MonitoredPrefix monitoredPrefix) {
        monitoredPrefixMap.put(prefix, monitoredPrefix);
        monitoredPrefixes.add(monitoredPrefix);
    }

    /**
     * Remove a monitored prefix
     *
     * @param prefix Prefix String
     */
    public void removeMonitoredPrefix(String prefix) {
        monitoredPrefixes.remove(monitoredPrefixMap.remove(prefix));
    }

    public boolean hasSeenBefore(String prefix, long seqNo) {
        MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
        if (monitoredPrefix != null) {
            long lastMessageSent = monitoredPrefix.getLastMessageSent();
            return lastMessageSent >= seqNo;
        }
        return false;
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

    /**
     * Update message rates for each monitored prefix
     *
     * @param timeElapsed time elapsed since last message rate calculation
     */
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

    /**
     * Get a list of prefixes for scaling out
     *
     * @param excess A measure of the excess load
     * @return List of prefixes chosen for scaling out
     */
    public List<String> getPrefixesForScalingOut(Double excess, boolean memoryBased) {
        List<String> prefixesForScalingOut = new ArrayList<>();
        double cumulSumOfPrefixes = 0;
        Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
        int locallyProcessedCount = 0;
        while (itr.hasNext() && cumulSumOfPrefixes < excess) {
            MonitoredPrefix monitoredPrefix = itr.next();
            if (!monitoredPrefix.getIsPassThroughTraffic()) {
                locallyProcessedCount++;
            }
            if (!monitoredPrefix.getIsPassThroughTraffic() && prefixesForScalingOut.size() < 25 &&
                    monitoredPrefix.getPrefix().length() <= AbstractGeoSpatialStreamProcessor.MAX_CHARACTER_DEPTH) {
                // let's consider the number of messages accumulated over 2s.
                if (memoryBased) {
                    cumulSumOfPrefixes += processor.getMemoryConsumptionForPrefix(monitoredPrefix.getPrefix());
                    prefixesForScalingOut.add(monitoredPrefix.getPrefix());
                } else {
                    cumulSumOfPrefixes += monitoredPrefix.getMessageRate() * 2;
                    prefixesForScalingOut.add(monitoredPrefix.getPrefix());
                }
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

    /**
     * Returns a list of prefixes for scaling in
     *
     * @param excess Extra load the current computation can take in
     * @return List of chosen prefixes
     */
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
     * Returns the set of child prefixes for corresponding to a given parent prefix
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

    public PendingScaleInRequest getPendingScalingInRequest(String key) {
        return pendingScaleInRequests.get(key);
    }

    public void removePendingScaleInRequest(String key) {
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

    /**
     * Get a list of outgoing streams that represent the entire child computation set.
     * For each child computation, the returned list contains one going stream
     *
     * @return List of outgoing streams covering all child computations
     */
    public List<String> getOutgoingStreams() {
        Set<String> uniqueComputations = new HashSet<>();
        List<String> outgoingStreams = new ArrayList<>();
        for (String monitoredPrefixStr : monitoredPrefixMap.keySet()) {
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(monitoredPrefixStr);
            if (monitoredPrefix.getIsPassThroughTraffic()) {
                if (!uniqueComputations.contains(monitoredPrefix.getDestComputationId())) {
                    uniqueComputations.add(monitoredPrefix.getDestComputationId());
                    outgoingStreams.add(monitoredPrefix.getOutGoingStream());
                }
            }
        }
        return outgoingStreams;
    }
}
