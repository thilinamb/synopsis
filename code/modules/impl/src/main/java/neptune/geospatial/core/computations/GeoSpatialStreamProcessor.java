package neptune.geospatial.core.computations;


import ds.funnel.topic.Topic;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.dataset.StreamEvent;
import ds.granules.exception.CommunicationsException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.protocol.msg.*;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.partitioner.GeoHashPartitioner;
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
public abstract class GeoSpatialStreamProcessor extends StreamProcessor {

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
        private AtomicBoolean isPassThroughTraffic;
        private String outGoingStream;
        private String destComputationId;
        private String destResourceCtrlEndpoint;
        private AtomicLong lastMessageSent;

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
    private class PendingScaleOutRequests {
        private List<String> prefixes;
        private String streamId;

        public PendingScaleOutRequests(List<String> prefixes, String streamId) {
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

    private Logger logger = Logger.getLogger(GeoSpatialStreamProcessor.class.getName());
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
    private Map<String, PendingScaleOutRequests> pendingScaleOutRequests = new HashMap<>();
    private AtomicReference<ArrayList<String>> lockedSubTrees = new AtomicReference<>(new ArrayList<String>());
    private Map<String, PendingScaleInRequest> pendingScaleInRequests = new HashMap<>();

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        if (!initialized.get()) {
            try {
                // register with the resource to enable monitoring
                initialized.set(true);
                messageSize.set(getMessageSize(streamEvent));
                ManagedResource.getInstance().registerStreamProcessor(this);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Initialized. Message Size: %d", getInstanceIdentifier(),
                            messageSize.get()));
                }
            } catch (NIException e) {
                logger.error("Error retrieving the resource instance.", e);
            }
        }
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        // preprocess each message
        if (preprocess(geoHashIndexedRecord)) {
            // perform the business logic: do this selectively. Send through the traffic we don't process.
            process(geoHashIndexedRecord);
        }
        process(geoHashIndexedRecord);
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
     */
    protected boolean preprocess(GeoHashIndexedRecord record) throws StreamingDatasetException {
        String prefix = getPrefix(record);
        updateIncomingRatesForSubPrefixes(prefix, record);
        boolean processLocally;
        synchronized (this) {
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
            // if there is an outgoing stream, then this should be sent to a child node.
            processLocally = !monitoredPrefix.isPassThroughTraffic.get();
            if (!processLocally) {
                monitoredPrefix.lastMessageSent.set(record.getMessageIdentifier());
                record.setPrefixLength(record.getPrefixLength() + 1);
                // send to the child node
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Forwarding Message. Prefix: %s, Outgoing Stream: %s",
                            getInstanceIdentifier(), prefix, monitoredPrefix.outGoingStream));
                }
                writeToStream(monitoredPrefix.outGoingStream, record);
            }
        }
        return processLocally;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }

    public synchronized void handleTriggerScaleAck(ScaleOutResponse ack) {
        if (pendingScaleOutRequests.containsKey(ack.getInResponseTo())) {
            PendingScaleOutRequests pendingReq = pendingScaleOutRequests.remove(ack.getInResponseTo());
            for (String prefix : pendingReq.prefixes) {
                MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
                monitoredPrefix.isPassThroughTraffic.set(true);
                monitoredPrefix.destComputationId = ack.getNewComputationId();
                monitoredPrefix.destResourceCtrlEndpoint = ack.getNewLocationURL();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] New Pass-Thru Prefix. Prefix: %s, Outgoing Stream: %s",
                            getInstanceIdentifier(), prefix, pendingReq.streamId));
                }
            }

            if (pendingScaleOutRequests.isEmpty()) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Scaling out complete for now.", getInstanceIdentifier()));
                    }
                    ManagedResource.getInstance().scalingOperationComplete(this.getInstanceIdentifier());
                } catch (NIException ignore) {

                }
            }
        } else {
            logger.warn("Invalid trigger ack for the prefix. Request Id: " + ack.getInResponseTo());
        }
    }

    private synchronized void updateIncomingRatesForSubPrefixes(String prefix, GeoHashIndexedRecord record) {
        MonitoredPrefix monitoredPrefix;
        if (monitoredPrefixMap.containsKey(prefix)) {
            monitoredPrefix = monitoredPrefixMap.get(prefix);
        } else {
            monitoredPrefix = new MonitoredPrefix(prefix, record.getClass().getName());
            monitoredPrefixes.add(monitoredPrefix);
            monitoredPrefixMap.put(prefix, monitoredPrefix);
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
        return record.getGeoHash().substring(0, record.getPrefixLength() + 1);
    }

    private int getIndexForSubPrefix(String geohash, int prefixLength) {
        return GEO_HASH_CHAR_SET.indexOf(geohash.charAt(prefixLength));
    }

    public long getBacklogLength() {
        return streamDataset.getQueueLengthInBytes() / messageSize.get();
    }

    /**
     * Resource recommends scaling out for one or more prefixes.
     */
    public synchronized void recommendScaling(double excess) throws ScalingException {
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
                } else {
                    // we couldn't find any suitable prefixes
                    ManagedResource.getInstance().scalingOperationComplete(getInstanceIdentifier());
                }
            } else {    // in the case of scaling down
                // find the prefixes with the lowest input rates that are pass-through traffic
                Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
                MonitoredPrefix chosenToScaleIn = null;
                while (itr.hasNext()) {
                    MonitoredPrefix monitoredPrefix = itr.next();
                    if (monitoredPrefix.isPassThroughTraffic.get() && monitoredPrefix.messageRate <
                            ManagedResource.LOW_THRESHOLD) {
                        // FIXME: Scale in just one computation, just to make sure protocol works
                        chosenToScaleIn = monitoredPrefix;
                        break;
                    }
                }
                if (chosenToScaleIn != null) {
                    initiateScaleIn(chosenToScaleIn);
                } else {
                    // we couldn't find any suitable prefixes
                    ManagedResource.getInstance().scalingOperationComplete(getInstanceIdentifier());
                }
            }
        } catch (NIException e) {
            throw handleError("Error getting the ManagedResource instance.", e);
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
            pendingScaleOutRequests.put(triggerMessage.getMessageId(), new PendingScaleOutRequests(prefix, outGoingStreamId));
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

    private void initiateScaleIn(MonitoredPrefix monitoredPrefix) throws ScalingException {
        // first check if the sub-tree is locked. This is possibly due to a request from the parent.
        boolean locked = false;
        for (String lockedPrefix : lockedSubTrees.get()) {
            if (monitoredPrefix.prefix.startsWith(lockedPrefix)) {
                locked = true;
                break;
            }
        }
        // tree is not locked. ask child nodes to lock.
        if (!locked) {
            ScaleInLockRequest lockReq = new ScaleInLockRequest(monitoredPrefix.prefix,
                    monitoredPrefix.destComputationId, getInstanceIdentifier());
            try {
                SendUtility.sendControlMessage(monitoredPrefix.destResourceCtrlEndpoint, lockReq);
                lockedSubTrees.get().add(monitoredPrefix.prefix);
                // book keeping of the sent out requests.
                PendingScaleInRequest pendingScaleInReq = new PendingScaleInRequest(monitoredPrefix.prefix, 1);
                pendingScaleInReq.sentOutRequests.put(monitoredPrefix.prefix, new QualifiedComputationAddr(
                        monitoredPrefix.destResourceCtrlEndpoint, monitoredPrefix.destComputationId));
                pendingScaleInRequests.put(monitoredPrefix.prefix, pendingScaleInReq);

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Sent a lock request. Prefix: %s, Destination: %s",
                            getInstanceIdentifier(), monitoredPrefix.prefix, monitoredPrefix.destResourceCtrlEndpoint));
                }
            } catch (CommunicationsException | IOException e) {
                String errorMsg = "Error sending out Lock Request to " + monitoredPrefix.destResourceCtrlEndpoint;
                throw handleError(errorMsg, e);
            }
        } else {
            // unable to get the lock. Already a scale-in operation is in progress.
            try {
                ManagedResource.getInstance().scalingOperationComplete(getInstanceIdentifier());
            } catch (NIException e) {
                String errMsg = "Error retrieving the ManagedResource instance.";
                logger.error(errMsg);
                throw new ScalingException(errMsg, e);
            }
        }
    }

    public synchronized void handleScaleInLockReq(ScaleInLockRequest lockReq) {
        String prefixForLock = lockReq.getPrefix();
        boolean lockAvailable = true;
        for (String lockedPrefix : lockedSubTrees.get()) {
            if (lockedPrefix.startsWith(prefixForLock)) {
                // we have already locked the subtree
                lockAvailable = false;
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Unable to grant lock for prefix: %s. Already locked for %s",
                            getInstanceIdentifier(), prefixForLock, lockedPrefix));
                }
                break;
            }
        }

        if (!lockAvailable) {
            ScaleInLockResponse lockResp = new ScaleInLockResponse(false, lockReq.getPrefix(),
                    lockReq.getSourceComputation());
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
                        ScaleInLockRequest childLockReq = new ScaleInLockRequest(prefixForLock, pref.destComputationId,
                                getInstanceIdentifier());
                        try {
                            SendUtility.sendControlMessage(pref.destResourceCtrlEndpoint, childLockReq);
                            lockedSubTrees.get().add(monitoredPrefix);
                            requestsSentOut.put(monitoredPrefix, new QualifiedComputationAddr(
                                    pref.destResourceCtrlEndpoint, pref.destComputationId));
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("[%s] Propagating lock requests to child elements. " +
                                                "Parent prefix: %s, Child Prefix: %s", getInstanceIdentifier(),
                                        prefixForLock, monitoredPrefix));
                            }
                        } catch (CommunicationsException | IOException e) {
                            logger.error("Error sending a lock request to child for prefix: " + monitoredPrefix);
                        }
                    } else {
                        locallyProcessedPrefixes.add(monitoredPrefix);
                    }
                }
            }
            // breaking condition of the recursive calls.
            if (requestsSentOut.size() == 0) {
                ScaleInLockResponse lockResp = new ScaleInLockResponse(true, prefixForLock,
                        lockReq.getSourceComputation());
                try {
                    SendUtility.sendControlMessage(lockReq.getOriginEndpoint(), lockReq);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending back the lock response to " + lockReq.getOriginEndpoint());
                }
            } else {
                PendingScaleInRequest pendingScaleInRequest = new PendingScaleInRequest(prefixForLock,
                        requestsSentOut.size(), lockReq.getOriginEndpoint(), lockReq.getSourceComputation());
                pendingScaleInRequest.locallyProcessedPrefixes = locallyProcessedPrefixes;
                pendingScaleInRequests.put(prefixForLock, pendingScaleInRequest);
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
            if (pendingReq.sentCount == pendingReq.receivedCount) {
                if (pendingReq.initiatedLocally) {
                    if (pendingReq.lockAcquired) {
                        for (String lockedPrefix : pendingReq.sentOutRequests.keySet()) {
                            // disable pass-through
                            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
                            monitoredPrefix.isPassThroughTraffic.set(false);
                            QualifiedComputationAddr reqInfo = pendingReq.sentOutRequests.get(lockedPrefix);
                            ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(lockedPrefix,
                                    reqInfo.computationId, monitoredPrefix.lastMessageSent.get());
                            try {
                                SendUtility.sendControlMessage(reqInfo.ctrlEndpointAddr, scaleInActivateReq);
                            } catch (CommunicationsException | IOException e) {
                                logger.error("Error sending ScaleInActivationRequest to " + reqInfo.ctrlEndpointAddr, e);
                            }
                        }
                    } else {
                        // haven't been able to acquire all the locks. Reset the locks.
                        resetLocks(pendingReq.sentOutRequests.keySet());
                    }
                } else { // send the parent the status
                    if (!pendingReq.lockAcquired) {
                        // some of the child locks were not granted. Reset.
                        resetLocks(pendingReq.sentOutRequests.keySet());
                    }
                    ScaleInLockResponse lockRespToParent = new ScaleInLockResponse(pendingReq.lockAcquired, pendingReq.prefix,
                            pendingReq.originComputation);
                    try {
                        SendUtility.sendControlMessage(pendingReq.originCtrlEndpoint, lockRespToParent);
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending ScaleInLockResponse to " + lockRespToParent);
                    }
                }
                pendingScaleInRequests.remove(prefix);
            }
        }
    }

    private void resetLocks(Set<String> lockedPrefixes) {
        for (String lockedPref : lockedPrefixes) {
            lockedSubTrees.get().remove(lockedPref);
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

    public void handleScaleInActivateReq(ScaleInActivateReq activationReq){
        String prefix  = activationReq.getPrefix();
        if (!pendingScaleInRequests.containsKey(prefix)) {
            logger.warn("Invalid ScaleInActivateReq for prefix: " + prefix);
        } else {
            PendingScaleInRequest pendingReq = pendingScaleInRequests.get(prefix);
            for (String lockedPrefix : pendingReq.sentOutRequests.keySet()) {
                // disable pass-through
                MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(prefix);
                monitoredPrefix.isPassThroughTraffic.set(false);
                QualifiedComputationAddr reqInfo = pendingReq.sentOutRequests.get(lockedPrefix);
                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(lockedPrefix,
                        reqInfo.computationId, monitoredPrefix.lastMessageSent.get());
                try {
                    SendUtility.sendControlMessage(reqInfo.ctrlEndpointAddr, scaleInActivateReq);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending ScaleInActivationRequest to " + reqInfo.ctrlEndpointAddr, e);
                }
            } for(String localPrefix : pendingReq.locallyProcessedPrefixes){
                // TODO: wait it processes the last message and start migrating the state.
            }
        }
    }
}




