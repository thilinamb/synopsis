package neptune.geospatial.core.computations;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.dataset.StreamEvent;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import neptune.geospatial.core.protocol.msg.*;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.hazelcast.type.SketchLocation;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import neptune.geospatial.util.Mutex;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stream processor specialized for geo-spatial data processing.
 * Specific computations may be implement by extending this
 * abstract class.
 * Part of scaling in/out is implemented in this class.
 *
 * @author Thilina Buddhika
 */
@SuppressWarnings("unused")
public abstract class AbstractGeoSpatialStreamProcessor extends StreamProcessor {

    /**
     * Represents a monitored prefix.
     * Used to keep track of the message rates for each prefix under the
     * purview of the current computation.
     */
    private class MonitoredPrefix implements Comparable<MonitoredPrefix> {
        private String prefix;
        private String streamType;
        private long messageCount;
        private double messageRate;
        private AtomicBoolean isPassThroughTraffic = new AtomicBoolean(false);
        private String outGoingStream;
        private String destComputationId;
        private String destResourceCtrlEndpoint;
        private AtomicLong lastMessageSent = new AtomicLong(0);
        private String lastGeoHashSent;
        private long terminationPoint = -1;
        private ScaleInActivateReq activateReq;

        public MonitoredPrefix(String prefix, String streamType) {
            this.prefix = prefix;
            this.streamType = streamType;
        }

        @Override
        public int compareTo(MonitoredPrefix o) {
            // ascending sort based on input rates
            if (this.messageRate == o.messageRate) {
                return this.prefix.compareTo(o.prefix);
            } else {
                return (int) (this.messageRate - o.messageRate);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MonitoredPrefix that = (MonitoredPrefix) o;
            return prefix.equals(that.prefix) && streamType.equals(that.streamType);
        }

        @Override
        public int hashCode() {
            int result = prefix.hashCode();
            result = 31 * result + streamType.hashCode();
            return result;
        }
    }

    /**
     * Pending Scale Out Requests.
     * A scale out request message is sent to the deployer and we are waiting
     * for a response.
     */
    private class PendingScaleOutRequest {
        private List<String> prefixes;
        private String streamId;
        private int ackCount;

        public PendingScaleOutRequest(List<String> prefixes, String streamId) {
            this.prefixes = prefixes;
            this.streamId = streamId;
        }
    }

    /**
     * Uniquely identifies each computation in the cluster
     * using its resource control endpoint address and
     * computation identifier.
     */
    private class QualifiedComputationAddr {
        private String ctrlEndpointAddr;
        private String computationId;

        public QualifiedComputationAddr(String ctrlEndpointAddr, String computationId) {
            this.ctrlEndpointAddr = ctrlEndpointAddr;
            this.computationId = computationId;
        }
    }

    private class PendingScaleInRequest {
        private String prefix;
        private int sentCount;
        private int receivedCount;
        private String originCtrlEndpoint;
        private String originComputation;
        private boolean initiatedLocally;
        private boolean lockAcquired = true;
        private Map<String, QualifiedComputationAddr> sentOutRequests = new HashMap<>();
        private List<String> locallyProcessedPrefixes = new ArrayList<>();
        private List<String> childLeafPrefixes = new ArrayList<>();

        public PendingScaleInRequest(String prefix, int sentCount, String originCtrlEndpoint, String originComputation) {
            this.prefix = prefix;
            this.sentCount = sentCount;
            this.originCtrlEndpoint = originCtrlEndpoint;
            this.originComputation = originComputation;
            this.initiatedLocally = false;
        }

        public PendingScaleInRequest(String prefix, int sentCount) {
            this.prefix = prefix;
            this.sentCount = sentCount;
            this.initiatedLocally = true;
        }
    }

    private Logger logger = Logger.getLogger(AbstractGeoSpatialStreamProcessor.class.getName());
    public static final String OUTGOING_STREAM_BASE_ID = "out-going";
    private static final String GEO_HASH_CHAR_SET = "0123456789bcdefghjkmnpqrstuvwxyz";
    public static final int GEO_HASH_LEN_IN_CHARS = 32;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;

    private AtomicInteger outGoingStreamIdSeqGenerator = new AtomicInteger(100);
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger messageSize = new AtomicInteger(-1);
    private AtomicLong tsLastUpdated = new AtomicLong(0);

    private Set<MonitoredPrefix> monitoredPrefixes = new TreeSet<>();
    private Map<String, MonitoredPrefix> monitoredPrefixMap = new HashMap<>();
    private Map<String, PendingScaleOutRequest> pendingScaleOutRequests = new HashMap<>();
    private AtomicReference<ArrayList<String>> lockedSubTrees = new AtomicReference<>(new ArrayList<String>());
    private Map<String, PendingScaleInRequest> pendingScaleInRequests = new HashMap<>();

    // mutex to ensure only a single scale in/out operations takes place at a given time
    private final Mutex mutex = new Mutex();

    // Hazelcast + prefix tree
    private HazelcastInstance hzInstance;

    // temporary counter to test scale-in and out.
    private AtomicLong scaleInOutTrigger = new AtomicLong(0);

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        if (!initialized.get()) {
            try {
                // register with the resource to enable monitoring
                initialized.set(true);
                messageSize.set(getMessageSize(streamEvent));
                ManagedResource.getInstance().registerStreamProcessor(this);
                hzInstance = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Initialized. Message Size: %d", getInstanceIdentifier(),
                            messageSize.get()));
                }
            } catch (NIException e) {
                logger.error("Error retrieving the resource instance.", e);
            } catch (HazelcastException e) {
                logger.error("Error retrieving the Hazelcast instance.", e);
            }
        }
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        // preprocess each message
        if (preprocess(geoHashIndexedRecord)) {
            // perform the business logic: do this selectively. Send through the traffic we don't process.
            process(geoHashIndexedRecord);
        }
        /*long count = scaleInOutTrigger.incrementAndGet();
        try {
            if (count % 20000000 != 0 && count % 5000000 == 0) {
                logger.debug("Scaling Out!");
                recommendScaling(10);
            }
            if (count % 20000000 == 0) {
                logger.debug("Scaling In!");
                recommendScaling(-10);
            }
        } catch (Exception ignore) {

        }*/
    }

    /**
     * Implement the specific business logic to process each
     * <code>GeohashIndexedRecord</code> message.
     *
     * @param event <code>GeoHashIndexedRecord</code> element
     */
    protected abstract void process(GeoHashIndexedRecord event);


    /**
     * Preprocess every record to extract meta-data such as triggering
     * scale out operations. This is prior to performing actual processing
     * on a message.
     *
     * @param record <code>GeoHashIndexedRecord</code> element
     * @return Whether to process the record locally(true) or not(false).
     */
    protected boolean preprocess(GeoHashIndexedRecord record) throws StreamingDatasetException {
        String prefix = getPrefix(record);
        boolean processLocally;
        synchronized (this) {
            updateIncomingRatesForSubPrefixes(prefix, record);
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
            // if there is an outgoing stream, then this should be sent to a child node.
            processLocally = !monitoredPrefix.isPassThroughTraffic.get();
            monitoredPrefix.lastMessageSent.set(record.getMessageIdentifier());
            monitoredPrefix.lastGeoHashSent = record.getGeoHash();
            if (!processLocally) {
                record.setPrefixLength(record.getPrefixLength() + 1);
                // send to the child node
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("[%s] Forwarding Message. Prefix: %s, Outgoing Stream: %s",
                            getInstanceIdentifier(), prefix, monitoredPrefix.outGoingStream));
                }
                writeToStream(monitoredPrefix.outGoingStream, record);
            }
            if (monitoredPrefix.terminationPoint == monitoredPrefix.lastMessageSent.get()) {
                propagateScaleInActivationRequests(monitoredPrefix.activateReq);
            }
        }
        return processLocally;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }

    private synchronized void updateIncomingRatesForSubPrefixes(String prefix, GeoHashIndexedRecord record) {
        MonitoredPrefix monitoredPrefix;
        if (monitoredPrefixMap.containsKey(prefix)) {
            monitoredPrefix = monitoredPrefixMap.get(prefix);
        } else {
            monitoredPrefix = new MonitoredPrefix(prefix, record.getClass().getName());
            monitoredPrefixes.add(monitoredPrefix);
            monitoredPrefixMap.put(prefix, monitoredPrefix);
            try {
                IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
                prefMap.put(prefix, new SketchLocation(getInstanceIdentifier(), getCtrlEndpoint(),
                        SketchLocation.MODE_REGISTER_NEW_PREFIX));
            } catch (GranulesConfigurationException e) {
                logger.error("Error publishing to Hazelcast.", e);
            }
        }

        monitoredPrefix.messageCount++;

        long timeNow = System.currentTimeMillis();
        if (tsLastUpdated.get() == 0) {
            tsLastUpdated.set(timeNow);
        } else if ((timeNow - tsLastUpdated.get()) > INPUT_RATE_UPDATE_INTERVAL) {
            for (String monitoredPrefStr : monitoredPrefixMap.keySet()) {
                monitoredPrefix = monitoredPrefixMap.get(monitoredPrefStr);
                double timeElapsed = (timeNow - tsLastUpdated.get()) * 1.0;
                DecimalFormat dFormat = new DecimalFormat();
                monitoredPrefix.messageRate = monitoredPrefix.messageCount * 1000.0 / timeElapsed;
                monitoredPrefix.messageCount = 0;
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("[%s] Prefix: %s, Message Rate: %.3f", getInstanceIdentifier(),
                            prefix, monitoredPrefix.messageRate));
                }
            }
            tsLastUpdated.set(timeNow);
        }
    }

    private String getPrefix(GeoHashIndexedRecord record) {
        return getPrefix(record.getGeoHash(), record.getPrefixLength());
    }

    private String getPrefix(String geohash, int prefixLength) {
        return geohash.substring(0, prefixLength + 1);
    }

    private int getIndexForSubPrefix(String geohash, int prefixLength) {
        return GEO_HASH_CHAR_SET.indexOf(geohash.charAt(prefixLength));
    }

    public long getBacklogLength() {
        return streamDataset.getQueueLengthInBytes() / messageSize.get();
    }

    /**
     * Resource recommends scaling out for one or more prefixes.
     *
     * @param excess Determines whether to scale in or out. Higher the magnitude, more prefixes need to
     *               be scaled in/out.
     * @return {@code true} if it triggered a scaling operation. {@code false} if no scaling operation is
     * triggered.
     */
    public synchronized boolean recommendScaling(double excess) {
        // try to get the lock first
        if (!mutex.tryAcquire()) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Unable to acquire the mutex for scale in/out operation. Excess: %.3f",
                        getInstanceIdentifier(), excess));
            }
            return false;
        }
        // if the lock is acquired successfully
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Successfully acquired the mutex for scale in/out operation. Excess: %.3f",
                    getInstanceIdentifier(), excess));
        }
        try {
            // in the case of scaling out
            if (excess > 0) {
                List<String> prefixesForScalingOut = new ArrayList<>();
                double cumulSumOfPrefixes = 0;
                Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
                while (itr.hasNext() && cumulSumOfPrefixes < excess) {
                    MonitoredPrefix monitoredPrefix = itr.next();
                    if (!monitoredPrefix.isPassThroughTraffic.get() && monitoredPrefix.messageRate > 0) {
                        prefixesForScalingOut.add(monitoredPrefix.prefix);
                        // let's consider the number of messages accumulated over 2s.
                        cumulSumOfPrefixes += monitoredPrefix.messageRate * 2;
                    }
                }
                if (logger.isDebugEnabled()) {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (String prefix : prefixesForScalingOut) {
                        stringBuilder.append(prefix).append("(").append(monitoredPrefixMap.get(prefix).messageRate).
                                append("), ");
                    }
                    logger.debug(String.format("[%s] Scale Out recommendation. Excess: %.3f, Chosen Prefixes: %s",
                            getInstanceIdentifier(), excess, stringBuilder.toString()));
                }
                if (!prefixesForScalingOut.isEmpty()) {
                    // We assume we use the same message type throughout the graph.
                    String streamType = monitoredPrefixMap.get(prefixesForScalingOut.get(0)).streamType;
                    initiateScaleOut(prefixesForScalingOut, streamType);
                    return true;
                } else {
                    // we couldn't find any suitable prefixes
                    mutex.release();
                    return false;
                }
            } else {    // in the case of scaling down
                // find the prefixes with the lowest input rates that are pass-through traffic
                Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
                MonitoredPrefix chosenToScaleIn = null;
                while (itr.hasNext()) {
                    MonitoredPrefix monitoredPrefix = itr.next();
                    if (monitoredPrefix.isPassThroughTraffic.get()) {
                        // FIXME: Scale in just one computation, just to make sure protocol works
                        chosenToScaleIn = monitoredPrefix;
                        break;
                    }
                }
                if (chosenToScaleIn != null) {
                    initiateScaleIn(chosenToScaleIn);
                    return true;
                } else {
                    mutex.release();
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Releasing the acquired lock for scaling in operation. " +
                                "Not outgoing prefixes.", getInstanceIdentifier()));
                    }
                    return false;
                }
            }
        } catch (Throwable e) { // Defending against any runtime exception.
            logger.error("Scaling Error.", e);
            mutex.release();
            return false;
        }
    }

    private void initiateScaleOut(List<String> prefix, String streamType) throws ScalingException {
        try {
            GeoHashPartitioner partitioner = new GeoHashPartitioner();
            String outGoingStreamId = getNewStreamIdentifier();
            declareStream(outGoingStreamId, streamType);
            // initialize the meta-data
            Topic[] topics = deployStream(outGoingStreamId, 1, partitioner);

            ScaleOutRequest triggerMessage = new ScaleOutRequest(getInstanceIdentifier(), outGoingStreamId,
                    topics[0].toString(), streamType);
            pendingScaleOutRequests.put(triggerMessage.getMessageId(), new PendingScaleOutRequest(prefix, outGoingStreamId));
            ManagedResource.getInstance().sendToDeployer(triggerMessage);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Sent a trigger scale message to deployer for the prefix: %s.",
                        getInstanceIdentifier(), prefix));
            }
        } catch (StreamingGraphConfigurationException e) {
            throw handleError("Error declaring a stream for scaling out.", e);
        } catch (StreamingDatasetException e) {
            throw handleError("Error deploying the new stream.", e);
        } catch (NIException e) {
            throw handleError("Error retrieving an instance of the ManagedResource.", e);
        }
    }

    public synchronized void handleTriggerScaleAck(ScaleOutResponse ack) {
        if (pendingScaleOutRequests.containsKey(ack.getInResponseTo())) {
            PendingScaleOutRequest pendingReq = pendingScaleOutRequests.get(ack.getInResponseTo());
            for (String prefix : pendingReq.prefixes) {
                MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
                monitoredPrefix.isPassThroughTraffic.set(true);
                monitoredPrefix.destComputationId = ack.getNewComputationId();
                monitoredPrefix.destResourceCtrlEndpoint = ack.getNewLocationURL();
                monitoredPrefix.outGoingStream = pendingReq.streamId;

                try {
                    byte[] state = split(prefix);
                    StateTransferMsg stateTransferMsg = new StateTransferMsg(prefix, ack.getInResponseTo(), state,
                            ack.getNewComputationId(), getInstanceIdentifier(), StateTransferMsg.SCALE_OUT);
                    stateTransferMsg.setLastMessageId(monitoredPrefix.lastMessageSent.get());
                    stateTransferMsg.setLastMessagePrefix(monitoredPrefix.lastGeoHashSent);
                    SendUtility.sendControlMessage(monitoredPrefix.destResourceCtrlEndpoint, stateTransferMsg);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] New Pass-Thru Prefix. Prefix: %s, Outgoing Stream: %s",
                                getInstanceIdentifier(), prefix, pendingReq.streamId));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error transferring state to " + monitoredPrefix.destResourceCtrlEndpoint);
                }
            }
        } else {
            logger.warn("Invalid trigger ack for the prefix. Request Id: " + ack.getInResponseTo());
        }
    }

    public synchronized void handleScaleOutCompleteAck(ScaleOutCompleteAck ack) {
        if (pendingScaleOutRequests.containsKey(ack.getKey())) {
            PendingScaleOutRequest pendingReq = pendingScaleOutRequests.get(ack.getKey());
            pendingReq.ackCount++;
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleOutCompleteAck. Sent Count: %d, Received Count: %d",
                        getInstanceIdentifier(), pendingReq.prefixes.size(), pendingReq.ackCount));
            }
            // update prefix tree
            /*IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(ack.getPrefix());
            prefMap.put(ack.getPrefix(), new SketchLocation(monitoredPrefix.destComputationId,
                    monitoredPrefix.destResourceCtrlEndpoint,
                    SketchLocation.MODE_SCALE_OUT));
            */
            if (pendingReq.ackCount == pendingReq.prefixes.size()) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Scaling out complete for now.", getInstanceIdentifier()));
                    }
                    // TODO: temporary fix to track dynamic scaling. Remove this and uncomment above after the micro-benchmark
                    IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
                    MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(ack.getPrefix());
                    prefMap.put(ack.getPrefix(), new SketchLocation(monitoredPrefix.destComputationId,
                            monitoredPrefix.destResourceCtrlEndpoint,
                            SketchLocation.MODE_SCALE_OUT));
                    pendingScaleOutRequests.remove(ack.getKey());
                    mutex.release();
                    ManagedResource.getInstance().scalingOperationComplete(this.getInstanceIdentifier());
                } catch (NIException ignore) {

                }
            }
        } else {
            logger.warn("Invalid ScaleOutCompleteAck for " + ack.getKey());
        }
    }

    private void initiateScaleIn(MonitoredPrefix monitoredPrefix) throws ScalingException {
        // first check if the sub-tree is locked. This is possibly due to a request from the parent.
        //boolean locked = true;
        /*for (String lockedPrefix : lockedSubTrees.get()) {
            if (monitoredPrefix.prefix.startsWith(lockedPrefix)) {
                locked = true;
                break;
            }
        }*/
        // tree is not locked. ask child nodes to lock.
        //if (!locked) {
        ScaleInLockRequest lockReq = new ScaleInLockRequest(monitoredPrefix.prefix,
                getInstanceIdentifier(), monitoredPrefix.destComputationId);
        try {
            SendUtility.sendControlMessage(monitoredPrefix.destResourceCtrlEndpoint, lockReq);
            //lockedSubTrees.get().add(monitoredPrefix.prefix);
            // book keeping of the sent out requests.
            PendingScaleInRequest pendingScaleInReq = new PendingScaleInRequest(monitoredPrefix.prefix, 1);
            pendingScaleInReq.sentOutRequests.put(monitoredPrefix.prefix, new QualifiedComputationAddr(
                    monitoredPrefix.destResourceCtrlEndpoint, monitoredPrefix.destComputationId));
            pendingScaleInRequests.put(monitoredPrefix.prefix, pendingScaleInReq);

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Sent a lock request. Prefix: %s, Destination Node: %s, " +
                                "Destination Computation: %s", getInstanceIdentifier(), monitoredPrefix.prefix,
                        monitoredPrefix.destResourceCtrlEndpoint, monitoredPrefix.destComputationId));
            }
        } catch (CommunicationsException | IOException e) {
            String errorMsg = "Error sending out Lock Request to " + monitoredPrefix.destResourceCtrlEndpoint;
            throw handleError(errorMsg, e);
        }
        /*} else {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Unable to acquire lock for prefix: %s", getInstanceIdentifier(),
                    monitoredPrefix.prefix));
        }
        // unable to get the lock. Already a scale-in operation is in progress.
        try {
            ManagedResource.getInstance().scalingOperationComplete(getInstanceIdentifier());
        } catch (NIException e) {
            String errMsg = "Error retrieving the ManagedResource instance.";
            logger.error(errMsg);
            throw new ScalingException(errMsg, e);
        }
        }*/
    }

    public synchronized void handleScaleInLockReq(ScaleInLockRequest lockReq) {
        String prefixForLock = lockReq.getPrefix();
        boolean lockAvailable = mutex.tryAcquire();
        /*for (String lockedPrefix : lockedSubTrees.get()) {
            if (lockedPrefix.startsWith(prefixForLock)) {
                // we have already locked the subtree
                lockAvailable = false;
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Unable to grant lock for prefix: %s. Already locked for %s",
                            getInstanceIdentifier(), prefixForLock, lockedPrefix));
                }
                break;
            }
        }*/

        if (!lockAvailable) {
            ScaleInLockResponse lockResp = new ScaleInLockResponse(false, lockReq.getPrefix(),
                    lockReq.getSourceComputation(), null);
            try {
                SendUtility.sendControlMessage(lockReq.getOriginEndpoint(), lockReq);
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending back the lock response to " + lockReq.getOriginEndpoint());
            }
        } else {
            // need to find out the available sub prefixes
            Map<String, QualifiedComputationAddr> requestsSentOut = new HashMap<>();
            List<String> locallyProcessedPrefixes = new ArrayList<>();
            for (String monitoredPrefix : monitoredPrefixMap.keySet()) {
                if (monitoredPrefix.startsWith(prefixForLock)) {
                    MonitoredPrefix pref = monitoredPrefixMap.get(monitoredPrefix);
                    if (pref.isPassThroughTraffic.get()) {
                        ScaleInLockRequest childLockReq = new ScaleInLockRequest(prefixForLock, getInstanceIdentifier(),
                                pref.destComputationId);
                        try {
                            SendUtility.sendControlMessage(pref.destResourceCtrlEndpoint, childLockReq);
                            //lockedSubTrees.get().add(monitoredPrefix);
                            requestsSentOut.put(monitoredPrefix, new QualifiedComputationAddr(
                                    pref.destResourceCtrlEndpoint, pref.destComputationId));
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("[%s] Propagating lock requests to child elements. " +
                                                "Parent prefix: %s, Child Prefix: %s, Child Resource: %s, " +
                                                "Child Computation Id: %s", getInstanceIdentifier(),
                                        prefixForLock, monitoredPrefix, pref.destResourceCtrlEndpoint,
                                        pref.destComputationId));
                            }
                        } catch (CommunicationsException | IOException e) {
                            logger.error("Error sending a lock request to child for prefix: " + monitoredPrefix);
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Found a locally processed prefix for ScaleInLock. " +
                                            "Parent Prefix: %s, Child Prefix: %s", getInstanceIdentifier(),
                                    prefixForLock, monitoredPrefix));
                        }
                        locallyProcessedPrefixes.add(monitoredPrefix);
                    }
                }
            }
            PendingScaleInRequest pendingScaleInRequest = new PendingScaleInRequest(prefixForLock,
                    requestsSentOut.size(), lockReq.getOriginEndpoint(), lockReq.getSourceComputation());
            pendingScaleInRequest.locallyProcessedPrefixes = locallyProcessedPrefixes;
            pendingScaleInRequest.sentOutRequests = requestsSentOut;
            pendingScaleInRequests.put(prefixForLock, pendingScaleInRequest);

            // breaking condition of the recursive calls.
            if (requestsSentOut.size() == 0) {
                ScaleInLockResponse lockResp = new ScaleInLockResponse(true, prefixForLock,
                        lockReq.getSourceComputation(), locallyProcessedPrefixes);
                try {
                    SendUtility.sendControlMessage(lockReq.getOriginEndpoint(), lockResp);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] No Child Sub-prefixes found for prefix: %s. " +
                                        "Acknowledging parent: %s, %s", getInstanceIdentifier(), prefixForLock,
                                lockReq.getOriginEndpoint(), lockReq.getSourceComputation()));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending back the lock response to " + lockReq.getOriginEndpoint());
                }
            }
        }
    }

    public synchronized void handleScaleInLockResponse(ScaleInLockResponse lockResponse) {
        String prefix = lockResponse.getPrefix();
        if (!pendingScaleInRequests.containsKey(prefix)) {
            logger.warn("Invalid ScaleInLockResponse for prefix: " + prefix);
        } else {
            PendingScaleInRequest pendingReq = pendingScaleInRequests.get(prefix);
            pendingReq.receivedCount++;
            pendingReq.lockAcquired = pendingReq.lockAcquired & lockResponse.isSuccess();
            if (lockResponse.getLeafPrefixes() != null) {
                pendingReq.childLeafPrefixes.addAll(lockResponse.getLeafPrefixes());
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleInLockResponse. Prefix: %s, " +
                                "SentCount: %d, ReceivedCount: %d, Lock Acquired: %b",
                        getInstanceIdentifier(), prefix, pendingReq.sentCount, pendingReq.receivedCount,
                        pendingReq.lockAcquired));
            }
            if (pendingReq.sentCount == pendingReq.receivedCount) {
                if (pendingReq.initiatedLocally) {
                    if (pendingReq.lockAcquired) {
                        for (String lockedPrefix : pendingReq.sentOutRequests.keySet()) {
                            // disable pass-through
                            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
                            monitoredPrefix.isPassThroughTraffic.set(false);
                            QualifiedComputationAddr reqInfo = pendingReq.sentOutRequests.get(lockedPrefix);
                            try {
                                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(prefix,
                                        reqInfo.computationId, monitoredPrefix.lastMessageSent.get(),
                                        monitoredPrefix.lastGeoHashSent, monitoredPrefix.prefix.length(),
                                        getCtrlEndpoint(), getInstanceIdentifier());
                                SendUtility.sendControlMessage(reqInfo.ctrlEndpointAddr, scaleInActivateReq);

                                if (logger.isDebugEnabled()) {
                                    logger.debug(String.format("[%s] Sent a ScaleInActivateReq for parent prefix: %s, " +
                                                    "child prefix: %s, To: %s -> %s, Last Message Sent: %d, Geohash: %s",
                                            getInstanceIdentifier(), prefix, monitoredPrefix.prefix, reqInfo.ctrlEndpointAddr,
                                            reqInfo.computationId, monitoredPrefix.lastMessageSent.get(),
                                            monitoredPrefix.lastGeoHashSent));
                                }
                            } catch (CommunicationsException | IOException | GranulesConfigurationException e) {
                                logger.error("Error sending ScaleInActivationRequest to " + reqInfo.ctrlEndpointAddr, e);
                            }
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Failed to acquire lock for prefix %s. Resetting locks.",
                                    getInstanceIdentifier(), prefix));
                        }
                        // haven't been able to acquire all the locks. Reset the locks.
                        completeScaleIn(prefix, pendingReq);
                    }
                } else { // send the parent the status
                    List<String> leafPrefixes = new ArrayList<>();
                    leafPrefixes.addAll(pendingReq.locallyProcessedPrefixes);
                    leafPrefixes.addAll(pendingReq.childLeafPrefixes);
                    ScaleInLockResponse lockRespToParent = new ScaleInLockResponse(pendingReq.lockAcquired, pendingReq.prefix,
                            pendingReq.originComputation, leafPrefixes);
                    try {
                        SendUtility.sendControlMessage(pendingReq.originCtrlEndpoint, lockRespToParent);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Acknowledging parent for lock status. " +
                                            "Prefix: %s, Lock Acquired: %b", getInstanceIdentifier(), prefix,
                                    pendingReq.lockAcquired));
                        }
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending ScaleInLockResponse to " + lockRespToParent);
                    }
                }
            }
        }
    }

    private String getNewStreamIdentifier() {
        return OUTGOING_STREAM_BASE_ID + "-" + outGoingStreamIdSeqGenerator.getAndIncrement();
    }

    private int getMessageSize(StreamEvent event) {
        try {
            String streamId = event.getStreamId();
            event.setStreamId(Integer.toString(0));
            byte[] marshalled = event.marshall();
            event.setStreamId(streamId);
            return marshalled.length;
        } catch (IOException e) {
            logger.error("Error calculating the message size using the first message.", e);
        }
        return -1;
    }

    private ScalingException handleError(String errorMsg, Throwable e) {
        logger.error(errorMsg, e);
        return new ScalingException(errorMsg, e);
    }

    public synchronized void handleScaleInActivateReq(ScaleInActivateReq activationReq) {
        String prefix = activationReq.getPrefix();
        if (!pendingScaleInRequests.containsKey(prefix)) {
            logger.warn("Invalid ScaleInActivateReq for prefix: " + prefix);
        } else {
            String lastMessagePrefix = getPrefix(activationReq.getLastGeoHashSent(),
                    activationReq.getCurrentPrefixLength());
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(lastMessagePrefix);
            if (monitoredPrefix != null) {
                if (monitoredPrefix.lastMessageSent.get() == activationReq.getLastMessageSent()) {
                    // we have already seen this message.
                    propagateScaleInActivationRequests(activationReq);
                } else {
                    monitoredPrefix.terminationPoint = activationReq.getLastMessageSent();
                    monitoredPrefix.activateReq = activationReq;
                }
            } else { // it is possible that the message has delivered yet, especially if there is a backlog
                monitoredPrefix = new MonitoredPrefix(lastMessagePrefix, null);
                monitoredPrefix.terminationPoint = activationReq.getLastMessageSent();
                monitoredPrefix.activateReq = activationReq;
                monitoredPrefixes.add(monitoredPrefix);
                monitoredPrefixMap.put(lastMessagePrefix, monitoredPrefix);
                // TODO: REMOVE
                for (String pref : monitoredPrefixMap.keySet()) {
                    logger.debug("************************ [" + pref + "] -> " +
                            monitoredPrefixMap.get(pref).lastMessageSent.get() + ": " +
                            monitoredPrefixMap.get(pref).lastGeoHashSent);
                }
                try {
                    IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
                    prefMap.put(lastMessagePrefix, new SketchLocation(getInstanceIdentifier(), getCtrlEndpoint(),
                            SketchLocation.MODE_REGISTER_NEW_PREFIX));
                } catch (GranulesConfigurationException e) {
                    logger.error("Error publishing to Hazelcast.", e);
                }
            }
        }
    }

    public synchronized void handleStateTransferReq(StateTransferMsg stateTransferMsg, boolean acked) {
        boolean scaleType = stateTransferMsg.isScaleType();
        if (scaleType) { // scale-in
            PendingScaleInRequest pendingReq = pendingScaleInRequests.get(stateTransferMsg.getKeyPrefix());
            pendingReq.childLeafPrefixes.remove(stateTransferMsg.getPrefix());
            merge(stateTransferMsg.getPrefix(), stateTransferMsg.getSerializedData());
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s]Received a StateTransferMsg. Prefix: %s, Key Prefix: %s, Remaining: %d",
                        getInstanceIdentifier(), stateTransferMsg.getPrefix(), stateTransferMsg.getKeyPrefix(),
                        pendingReq.childLeafPrefixes.size()));
            }
            if (pendingReq.childLeafPrefixes.isEmpty()) {
                completeScaleIn(stateTransferMsg.getKeyPrefix(), pendingReq);
            }
        } else {    // scale-out
            merge(stateTransferMsg.getPrefix(), stateTransferMsg.getSerializedData());
            String childPrefix = getPrefix(stateTransferMsg.getLastMessagePrefix(),
                    stateTransferMsg.getPrefix().length() + 1);
            // handling the case where no messages are sent after scaling out.
            if (!monitoredPrefixMap.containsKey(childPrefix)) {
                MonitoredPrefix monitoredPrefix = new MonitoredPrefix(childPrefix, null);
                monitoredPrefix.lastMessageSent.set(stateTransferMsg.getLastMessageId());
                monitoredPrefixes.add(monitoredPrefix);
                monitoredPrefixMap.put(childPrefix, monitoredPrefix);
                try {
                    IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
                    prefMap.put(childPrefix, new SketchLocation(getInstanceIdentifier(), getCtrlEndpoint(),
                            SketchLocation.MODE_REGISTER_NEW_PREFIX));
                } catch (GranulesConfigurationException e) {
                    logger.error("Error publishing to Hazelcast.", e);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Messages haven't arrived after scaling out. " +
                                    "Setting last message scene to: %d", getInstanceIdentifier(),
                            stateTransferMsg.getLastMessageId()));
                }
            }
            if (!acked) {
                ScaleOutCompleteAck ack = new ScaleOutCompleteAck(
                        stateTransferMsg.getKeyPrefix(), stateTransferMsg.getPrefix(),
                        stateTransferMsg.getOriginComputation());
                try {
                    SendUtility.sendControlMessage(stateTransferMsg.getOriginEndpoint(), ack);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error acknowledging the parent on the state transfer.", e);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a state transfer message for the prefix: %s during the " +
                        "scaling out.", getInstanceIdentifier(), stateTransferMsg.getPrefix()));
            }
        }
    }

    private void completeScaleIn(String prefix, PendingScaleInRequest pendingReq) {
        // initiate the scale-in complete request.
        for (Map.Entry<String, QualifiedComputationAddr> participant : pendingReq.sentOutRequests.entrySet()) {
            ScaleInComplete scaleInComplete = new ScaleInComplete(prefix, participant.getValue().computationId,
                    getInstanceIdentifier());
            try {
                SendUtility.sendControlMessage(participant.getValue().ctrlEndpointAddr, scaleInComplete);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received all StateTransferMsgs. " +
                                    "Initiating ProtocolEnd message flow. Prefix: %s",
                            getInstanceIdentifier(), prefix));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending out ScaleInComplete to " + participant.getValue().ctrlEndpointAddr, e);
            }
        }
        pendingReq.receivedCount = 0;
    }

    public synchronized void handleScaleInCompleteMsg(ScaleInComplete scaleInCompleteMsg) {
        PendingScaleInRequest pendingReq = pendingScaleInRequests.get(scaleInCompleteMsg.getPrefix());
        if (pendingReq.sentOutRequests.entrySet().size() > 0) {
            pendingReq.receivedCount = 0;
            for (Map.Entry<String, QualifiedComputationAddr> participant : pendingReq.sentOutRequests.entrySet()) {
                try {
                    ScaleInComplete scaleInComplete = new ScaleInComplete(scaleInCompleteMsg.getPrefix(),
                            participant.getValue().computationId, getInstanceIdentifier());
                    SendUtility.sendControlMessage(participant.getValue().ctrlEndpointAddr, scaleInComplete);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Forwarding the ScaleInComplete message to child nodes. " +
                                        "Key Prefix: %s, Child Prefix: %s", getInstanceIdentifier(),
                                scaleInCompleteMsg.getPrefix(), participant.getKey()));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending out ScaleInComplete to " + participant.getValue().ctrlEndpointAddr, e);
                }
            }
        } else {
            try {
                ScaleInCompleteAck ack = new ScaleInCompleteAck(scaleInCompleteMsg.getPrefix(),
                        scaleInCompleteMsg.getParentComputation());
                SendUtility.sendControlMessage(scaleInCompleteMsg.getOriginEndpoint(), ack);
                pendingScaleInRequests.remove(scaleInCompleteMsg.getPrefix());
                mutex.release();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] No Child Prefixes. Unlocking the mutex at node.",
                            getInstanceIdentifier()));
                }
            } catch (CommunicationsException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized void handleScaleInCompleteAck(ScaleInCompleteAck ack) {
        PendingScaleInRequest pendingReq = pendingScaleInRequests.get(ack.getPrefix());
        if (pendingReq != null) {
            pendingReq.receivedCount++;
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleInCompleteAck from a child. " +
                                "Sent Ack Count: %d, Received Ack Count: %d", getInstanceIdentifier(),
                        pendingReq.sentCount, pendingReq.receivedCount));
            }
            if (pendingReq.receivedCount == pendingReq.sentCount) {
                if (!pendingReq.initiatedLocally) {
                    ScaleInCompleteAck ackToParent = new ScaleInCompleteAck(ack.getPrefix(),
                            pendingReq.originComputation);
                    try {
                        SendUtility.sendControlMessage(pendingReq.originCtrlEndpoint, ackToParent);
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending out a ScaleInCompleteAck to " + pendingReq.originCtrlEndpoint);
                    }
                } else { // initiated locally.
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Completed Scaling in for prefix : %s",
                                getInstanceIdentifier(), ack.getPrefix()));
                    }
                    // update hazelcast
                    try {
                        IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
                        prefMap.put(ack.getPrefix(), new SketchLocation(getInstanceIdentifier(), getCtrlEndpoint(),
                                SketchLocation.MODE_SCALE_IN));
                    } catch (GranulesConfigurationException e) {
                        logger.error("Error publishing to Hazelcast.", e);
                    }
                }
                pendingScaleInRequests.remove(ack.getPrefix());
                mutex.release();
                try {
                    ManagedResource.getInstance().scalingOperationComplete(this.getInstanceIdentifier());
                } catch (NIException ignore) {

                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received all child ScaleInCompleteAcks. Acknowledging parent.",
                            getInstanceIdentifier()));
                }
            }
        }
    }

    private void propagateScaleInActivationRequests(ScaleInActivateReq activationReq) {
        String prefix = activationReq.getPrefix();
        PendingScaleInRequest pendingReq = pendingScaleInRequests.get(prefix);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Received ScaleInActivateReq for prefix: %s", getInstanceIdentifier(),
                    prefix));
        }
        for (String lockedPrefix : pendingReq.sentOutRequests.keySet()) {
            // disable pass-through
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
            monitoredPrefix.isPassThroughTraffic.set(false);
            QualifiedComputationAddr reqInfo = pendingReq.sentOutRequests.get(lockedPrefix);

            try {
                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(prefix, reqInfo.computationId,
                        monitoredPrefix.lastMessageSent.get(), monitoredPrefix.lastGeoHashSent, lockedPrefix.length(),
                        activationReq.getOriginNodeOfScalingOperation(),
                        activationReq.getOriginComputationOfScalingOperation());
                SendUtility.sendControlMessage(reqInfo.ctrlEndpointAddr, scaleInActivateReq);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Propagating ScaleInActivateReq to children. " +
                                    "Parent prefix: %s, Child prefix: %s, Last message processed: %d",
                            getInstanceIdentifier(), prefix, lockedPrefix, monitoredPrefix.lastMessageSent.get()));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending ScaleInActivationRequest to " + reqInfo.ctrlEndpointAddr, e);
            }
        }
        for (String localPrefix : pendingReq.locallyProcessedPrefixes) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] ScaleInActivationReq for locally processed prefix. " +
                                "Parent Prefix: %s, Child Prefix: %s, Last Processed Sent: %d",
                        getInstanceIdentifier(), prefix, localPrefix, activationReq.getLastMessageSent()));
            }
            // get the state for the prefix in the serialized form
            byte[] state = split(localPrefix);
            StateTransferMsg stateTransMsg = new StateTransferMsg(localPrefix, prefix, state,
                    activationReq.getOriginComputationOfScalingOperation(), getInstanceIdentifier(),
                    StateTransferMsg.SCALE_IN);
            try {
                SendUtility.sendControlMessage(activationReq.getOriginNodeOfScalingOperation(), stateTransMsg);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] StateTransfer for local prefix: %s. Key prefix: %s.",
                            getInstanceIdentifier(), localPrefix, prefix));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error when sending out the StateTransfer message to " +
                        activationReq.getOriginNodeOfScalingOperation());
            }
        }
    }

    private String getCtrlEndpoint() throws GranulesConfigurationException {
        try {
            return RivuletUtil.getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(
                            Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT);
        } catch (GranulesConfigurationException e) {
            logger.error("Error when retrieving the hostname.", e);
            throw e;
        }
    }

    public abstract byte[] split(String prefix);

    public abstract void merge(String prefix, byte[] serializedSketch);
}




