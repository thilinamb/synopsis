package neptune.geospatial.core.computations;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.dataset.StreamEvent;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.computations.scalingctxt.*;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    private Logger logger = Logger.getLogger(AbstractGeoSpatialStreamProcessor.class.getName());
    public static final String OUTGOING_STREAM_BASE_ID = "out-going";
    private static final String GEO_HASH_CHAR_SET = "0123456789bcdefghjkmnpqrstuvwxyz";
    public static final int MAX_CHARACTER_DEPTH = 4;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;

    private AtomicInteger outGoingStreamIdSeqGenerator = new AtomicInteger(100);
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger messageSize = new AtomicInteger(-1);
    private AtomicLong tsLastUpdated = new AtomicLong(0);

    private ScalingContext scalingContext = new ScalingContext(getInstanceIdentifier());

    // mutex to ensure only a single scale in/out operations takes place at a given time
    private final Mutex mutex = new Mutex();

    // Hazelcast + prefix tree
    private HazelcastInstance hzInstance;

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
        // this a dummy message sent to activate the computation after scaling out.
        if (geoHashIndexedRecord.getMessageIdentifier() == -1) {
            return;
        }
        // preprocess each message
        if (preprocess(geoHashIndexedRecord)) {
            // perform the business logic: do this selectively. Send through the traffic we don't process.
            process(geoHashIndexedRecord);
        }
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
            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
            // if there is an outgoing stream, then this should be sent to a child node.
            processLocally = !monitoredPrefix.getIsPassThroughTraffic();
            monitoredPrefix.setLastMessageSent(record.getMessageIdentifier());
            monitoredPrefix.setLastGeoHashSent(record.getGeoHash());
            if (!processLocally) {
                record.setPrefixLength(record.getPrefixLength() + 1);
                // send to the child node
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("[%s] Forwarding Message. Prefix: %s, Outgoing Stream: %s",
                            getInstanceIdentifier(), prefix, monitoredPrefix.getOutGoingStream()));
                }
                try {
                    writeToStream(monitoredPrefix.getOutGoingStream(), record);
                } catch (StreamingDatasetException e) {
                    logger.error("Error writing to stream to " + monitoredPrefix.getDestResourceCtrlEndpoint() + ":" +
                            monitoredPrefix.getDestComputationId());
                    throw e;
                }
            }
            if (monitoredPrefix.getTerminationPoint() == monitoredPrefix.getLastMessageSent()) {
                propagateScaleInActivationRequests(monitoredPrefix.getActivateReq());
            }
        }
        return processLocally;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges at the beginning
    }

    private synchronized void updateIncomingRatesForSubPrefixes(String prefix, GeoHashIndexedRecord record) {
        scalingContext.updateMessageCount(prefix, record.getClass().getName());
        long timeNow = System.currentTimeMillis();
        if (tsLastUpdated.get() == 0) {
            tsLastUpdated.set(timeNow);
        } else if ((timeNow - tsLastUpdated.get()) > INPUT_RATE_UPDATE_INTERVAL) {
            double timeElapsed = (timeNow - tsLastUpdated.get()) * 1.0;
            scalingContext.updateMessageRates(timeElapsed);
            tsLastUpdated.set(timeNow);
        }
    }

    private String getPrefix(GeoHashIndexedRecord record) {
        return getPrefix(record.getGeoHash(), record.getPrefixLength());
    }

    public String getPrefix(String geohash, int prefixLength) {
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
                List<String> prefixesForScalingOut = scalingContext.getPrefixesForScalingOut(excess);
                if (!prefixesForScalingOut.isEmpty()) {
                    // We assume we use the same message type throughout the graph.
                    String streamType = scalingContext.getMonitoredPrefix(prefixesForScalingOut.get(0)).getStreamType();
                    initiateScaleOut(prefixesForScalingOut, streamType);
                    return true;
                } else {
                    // we couldn't find any suitable prefixes
                    mutex.release();
                    return false;
                }
            } else {    // in the case of scaling down
                List<String> chosenToScaleIn = scalingContext.getPrefixesForScalingIn(excess);
                if (chosenToScaleIn.size() > 0) {
                    for (String chosenPrefix : chosenToScaleIn) {
                        initiateScaleIn(scalingContext.getMonitoredPrefix(chosenPrefix));
                    }
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
            scalingContext.addPendingScaleOutRequest(triggerMessage.getMessageId(), new PendingScaleOutRequest(
                    prefix, outGoingStreamId));
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

    public synchronized void handleTriggerScaleAck(ScaleOutResponse scaleOutResp) {
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(scaleOutResp.getInResponseTo());
        if (pendingReq != null) {
            for (String prefix : pendingReq.getPrefixes()) {
                MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
                monitoredPrefix.setIsPassThroughTraffic(true);
                monitoredPrefix.setDestComputationId(scaleOutResp.getNewComputationId());
                monitoredPrefix.setDestResourceCtrlEndpoint(scaleOutResp.getNewLocationURL());
                monitoredPrefix.setOutGoingStream(pendingReq.getStreamId());
                try {
                    // send a dummy message, just to ensure the new computation is activated.
                    GeoHashIndexedRecord record = new GeoHashIndexedRecord(monitoredPrefix.getLastGeoHashSent(),
                            prefix.length() + 1, -1, System.currentTimeMillis(), new byte[0]);
                    writeToStream(monitoredPrefix.getOutGoingStream(), record);
                    byte[] state = split(prefix);
                    StateTransferMsg stateTransferMsg = new StateTransferMsg(prefix, scaleOutResp.getInResponseTo(), state,
                            scaleOutResp.getNewComputationId(), getInstanceIdentifier(), StateTransferMsg.SCALE_OUT);
                    stateTransferMsg.setLastMessageId(monitoredPrefix.getLastMessageSent());
                    stateTransferMsg.setLastMessagePrefix(monitoredPrefix.getLastGeoHashSent());
                    SendUtility.sendControlMessage(monitoredPrefix.getDestResourceCtrlEndpoint(), stateTransferMsg);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] New Pass-Thru Prefix. Prefix: %s, Outgoing Stream: %s",
                                getInstanceIdentifier(), prefix, pendingReq.getStreamId()));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error transferring state to " + monitoredPrefix.getDestResourceCtrlEndpoint());
                } catch (StreamingDatasetException e) {
                    logger.error("Error sending a message");
                }
            }
        } else {
            logger.warn("Invalid trigger scaleOutResp for the prefix. Request Id: " + scaleOutResp.getInResponseTo());
        }
    }

    public synchronized void handleScaleOutCompleteAck(ScaleOutCompleteAck ack) {
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(ack.getKey());
        if (pendingReq != null) {
            int ackCount = pendingReq.incrementAndGetAckCount();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleOutCompleteAck. Sent Count: %d, Received Count: %d",
                        getInstanceIdentifier(), pendingReq.getPrefixes().size(), ackCount));
            }
            // update prefix tree
            /*IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(ack.getPrefix());
            prefMap.put(ack.getPrefix(), new SketchLocation(monitoredPrefix.destComputationId,
                    monitoredPrefix.destResourceCtrlEndpoint,
                    SketchLocation.MODE_SCALE_OUT));
            */
            if (ackCount == pendingReq.getPrefixes().size()) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Scaling out complete for now.", getInstanceIdentifier()));
                    }
                    // TODO: temporary fix to track dynamic scaling. Remove this and uncomment above after the micro-benchmark
                    IQueue<Integer> scalingMonitoringQueue = getHzInstance().getQueue("scaling-monitor");
                    scalingMonitoringQueue.add(ackCount);
                    scalingContext.completeScalingOut(ack.getKey());
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
        String prefix = monitoredPrefix.getPrefix();
        ScaleInLockRequest lockReq = new ScaleInLockRequest(prefix, getInstanceIdentifier(),
                monitoredPrefix.getDestComputationId());
        try {
            SendUtility.sendControlMessage(monitoredPrefix.getDestResourceCtrlEndpoint(), lockReq);
            // book keeping of the sent out requests.
            PendingScaleInRequest pendingScaleInReq = new PendingScaleInRequest(prefix, 1);
            pendingScaleInReq.addSentOutRequest(prefix, new FullQualifiedComputationAddr(
                    monitoredPrefix.getDestResourceCtrlEndpoint(), monitoredPrefix.getDestComputationId()));
            scalingContext.addPendingScalingInRequest(prefix, pendingScaleInReq);

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Sent a lock request. Prefix: %s, Destination Node: %s, " +
                                "Destination Computation: %s", getInstanceIdentifier(), prefix,
                        monitoredPrefix.getDestResourceCtrlEndpoint(), monitoredPrefix.getDestComputationId()));
            }
        } catch (CommunicationsException | IOException e) {
            String errorMsg = "Error sending out Lock Request to " + monitoredPrefix.getDestResourceCtrlEndpoint();
            throw handleError(errorMsg, e);
        }
    }

    public synchronized void handleScaleInLockReq(ScaleInLockRequest lockReq) {
        String prefixForLock = lockReq.getPrefix();
        boolean lockAvailable = mutex.tryAcquire();
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
            Map<String, FullQualifiedComputationAddr> requestsSentOut = new HashMap<>();
            List<String> locallyProcessedPrefixes = new ArrayList<>();
            List<MonitoredPrefix> childPrefixes = scalingContext.getChildPrefixesForScalingIn(prefixForLock);
            for (MonitoredPrefix pref : childPrefixes) {
                if (pref.getIsPassThroughTraffic()) {
                    ScaleInLockRequest childLockReq = new ScaleInLockRequest(prefixForLock, getInstanceIdentifier(),
                            pref.getDestComputationId());
                    try {
                        SendUtility.sendControlMessage(pref.getDestResourceCtrlEndpoint(), childLockReq);
                        //lockedSubTrees.get().add(monitoredPrefix);
                        requestsSentOut.put(pref.getPrefix(), new FullQualifiedComputationAddr(
                                pref.getDestResourceCtrlEndpoint(), pref.getDestComputationId()));
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Propagating lock requests to child elements. " +
                                            "Parent prefix: %s, Child Prefix: %s, Child Resource: %s, " +
                                            "Child Computation Id: %s", getInstanceIdentifier(),
                                    prefixForLock, pref.getPrefix(), pref.getDestResourceCtrlEndpoint(),
                                    pref.getDestComputationId()));
                        }
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending a lock request to child for prefix: " + pref.getPrefix());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Found a locally processed prefix for ScaleInLock. " +
                                        "Parent Prefix: %s, Child Prefix: %s", getInstanceIdentifier(),
                                prefixForLock, pref.getPrefix()));
                    }
                    locallyProcessedPrefixes.add(pref.getPrefix());
                }

            }
            PendingScaleInRequest pendingScaleInRequest = new PendingScaleInRequest(prefixForLock,
                    requestsSentOut.size(), lockReq.getOriginEndpoint(), lockReq.getSourceComputation());
            pendingScaleInRequest.setLocallyProcessedPrefix(locallyProcessedPrefixes);
            pendingScaleInRequest.setSentOutRequests(requestsSentOut);
            scalingContext.addPendingScalingInRequest(prefixForLock, pendingScaleInRequest);

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
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(prefix);

        if (pendingReq == null) {
            logger.warn("Invalid ScaleInLockResponse for prefix: " + prefix);
        } else {
            int receivedCount = pendingReq.incrementAndGetReceivedCount();
            boolean lockAcquired = pendingReq.updateAndGetLockStatus(lockResponse.isSuccess());
            if (lockResponse.getLeafPrefixes() != null) {
                pendingReq.addChildPrefixes(lockResponse.getLeafPrefixes());
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleInLockResponse. Prefix: %s, " +
                                "SentCount: %d, ReceivedCount: %d, Lock Acquired: %b",
                        getInstanceIdentifier(), prefix, pendingReq.getSentCount(), receivedCount, lockAcquired));
            }
            if (pendingReq.getSentCount() == receivedCount) {
                if (pendingReq.isInitiatedLocally()) {
                    if (lockAcquired) {
                        for (String lockedPrefix : pendingReq.getSentOutRequests().keySet()) {
                            // disable pass-through
                            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
                            monitoredPrefix.setIsPassThroughTraffic(false);
                            FullQualifiedComputationAddr reqInfo = pendingReq.getSentOutRequests().get(lockedPrefix);
                            try {
                                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(prefix,
                                        reqInfo.getComputationId(), monitoredPrefix.getLastMessageSent(),
                                        monitoredPrefix.getLastGeoHashSent(), monitoredPrefix.getPrefix().length(),
                                        RivuletUtil.getCtrlEndpoint(), getInstanceIdentifier());
                                SendUtility.sendControlMessage(reqInfo.getCtrlEndpointAddr(), scaleInActivateReq);

                                if (logger.isDebugEnabled()) {
                                    logger.debug(String.format("[%s] Sent a ScaleInActivateReq for parent prefix: %s, " +
                                                    "child prefix: %s, To: %s -> %s, Last Message Sent: %d, Geohash: %s",
                                            getInstanceIdentifier(), prefix, monitoredPrefix.getPrefix(),
                                            reqInfo.getCtrlEndpointAddr(), reqInfo.getComputationId(),
                                            monitoredPrefix.getLastMessageSent(),
                                            monitoredPrefix.getLastGeoHashSent()));
                                }
                            } catch (CommunicationsException | IOException | GranulesConfigurationException e) {
                                logger.error("Error sending ScaleInActivationRequest to " +
                                        reqInfo.getCtrlEndpointAddr(), e);
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
                    leafPrefixes.addAll(pendingReq.getLocallyProcessedPrefixes());
                    leafPrefixes.addAll(pendingReq.getChildLeafPrefixes());
                    ScaleInLockResponse lockRespToParent = new ScaleInLockResponse(lockAcquired, pendingReq.getPrefix(),
                            pendingReq.getOriginComputation(), leafPrefixes);
                    try {
                        SendUtility.sendControlMessage(pendingReq.getOriginCtrlEndpoint(), lockRespToParent);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Acknowledging parent for lock status. " +
                                            "Prefix: %s, Lock Acquired: %b", getInstanceIdentifier(), prefix,
                                    lockAcquired));
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
        if (scalingContext.getPendingScalingInRequest(prefix) == null) {
            logger.warn("Invalid ScaleInActivateReq for prefix: " + prefix);
        } else {
            String lastMessagePrefix = getPrefix(activationReq.getLastGeoHashSent(),
                    activationReq.getCurrentPrefixLength());
            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(lastMessagePrefix);
            if (monitoredPrefix != null) {
                if (monitoredPrefix.getLastMessageSent() == activationReq.getLastMessageSent()) {
                    // we have already seen this message.
                    propagateScaleInActivationRequests(activationReq);
                } else {
                    monitoredPrefix.setTerminationPoint(activationReq.getLastMessageSent());
                    monitoredPrefix.setActivateReq(activationReq);
                }
            } else { // it is possible that the message has delivered yet, especially if there is a backlog
                monitoredPrefix = new MonitoredPrefix(lastMessagePrefix, null);
                monitoredPrefix.setTerminationPoint(activationReq.getLastMessageSent());
                monitoredPrefix.setActivateReq(activationReq);
                scalingContext.addMonitoredPrefix(lastMessagePrefix, monitoredPrefix);
                try {
                    IMap<String, SketchLocation> prefMap = getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                    prefMap.put(lastMessagePrefix, new SketchLocation(getInstanceIdentifier(),
                            RivuletUtil.getCtrlEndpoint(), SketchLocation.MODE_REGISTER_NEW_PREFIX));
                } catch (GranulesConfigurationException e) {
                    logger.error("Error publishing to Hazelcast.", e);
                }
            }
        }
    }

    public synchronized void handleStateTransferReq(StateTransferMsg stateTransferMsg, boolean acked) {
        boolean scaleType = stateTransferMsg.isScaleType();
        if (scaleType) { // scale-in
            PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(stateTransferMsg.getKeyPrefix());
            pendingReq.getChildLeafPrefixes().remove(stateTransferMsg.getPrefix());
            merge(stateTransferMsg.getPrefix(), stateTransferMsg.getSerializedData());
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s]Received a StateTransferMsg. Prefix: %s, Key Prefix: %s, Remaining: %d",
                        getInstanceIdentifier(), stateTransferMsg.getPrefix(), stateTransferMsg.getKeyPrefix(),
                        pendingReq.getChildLeafPrefixes().size()));
            }
            if (pendingReq.getChildLeafPrefixes().isEmpty()) {
                completeScaleIn(stateTransferMsg.getKeyPrefix(), pendingReq);
            }
        } else {    // scale-out
            merge(stateTransferMsg.getPrefix(), stateTransferMsg.getSerializedData());
            String childPrefix = getPrefix(stateTransferMsg.getLastMessagePrefix(),
                    stateTransferMsg.getPrefix().length());
            // handling the case where no messages are sent after scaling out.
            if (scalingContext.getMonitoredPrefix(childPrefix) == null) {
                MonitoredPrefix monitoredPrefix = new MonitoredPrefix(childPrefix, null);
                monitoredPrefix.setLastMessageSent(stateTransferMsg.getLastMessageId());
                scalingContext.addMonitoredPrefix(childPrefix, monitoredPrefix);
                try {
                    IMap<String, SketchLocation> prefMap = getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                    prefMap.put(childPrefix, new SketchLocation(getInstanceIdentifier(), RivuletUtil.getCtrlEndpoint(),
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
        for (Map.Entry<String, FullQualifiedComputationAddr> participant : pendingReq.getSentOutRequests().entrySet()) {
            ScaleInComplete scaleInComplete = new ScaleInComplete(prefix, participant.getValue().getComputationId(),
                    getInstanceIdentifier());
            try {
                SendUtility.sendControlMessage(participant.getValue().getCtrlEndpointAddr(), scaleInComplete);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received all StateTransferMsgs. " +
                                    "Initiating ProtocolEnd message flow. Prefix: %s",
                            getInstanceIdentifier(), prefix));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending out ScaleInComplete to " + participant.getValue().getCtrlEndpointAddr(), e);
            }
        }
        pendingReq.setReceivedCount(0);
    }

    public synchronized void handleScaleInCompleteMsg(ScaleInComplete scaleInCompleteMsg) {
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(scaleInCompleteMsg.getPrefix());
        if (pendingReq.getSentOutRequests().entrySet().size() > 0) {
            pendingReq.setReceivedCount(0);
            for (Map.Entry<String, FullQualifiedComputationAddr> participant : pendingReq.getSentOutRequests().entrySet()) {
                try {
                    ScaleInComplete scaleInComplete = new ScaleInComplete(scaleInCompleteMsg.getPrefix(),
                            participant.getValue().getComputationId(), getInstanceIdentifier());
                    SendUtility.sendControlMessage(participant.getValue().getCtrlEndpointAddr(), scaleInComplete);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Forwarding the ScaleInComplete message to child nodes. " +
                                        "Key Prefix: %s, Child Prefix: %s", getInstanceIdentifier(),
                                scaleInCompleteMsg.getPrefix(), participant.getKey()));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending out ScaleInComplete to " + participant.getValue().getCtrlEndpointAddr(), e);
                }
            }
        } else {
            try {
                ScaleInCompleteAck ack = new ScaleInCompleteAck(scaleInCompleteMsg.getPrefix(),
                        scaleInCompleteMsg.getParentComputation());
                SendUtility.sendControlMessage(scaleInCompleteMsg.getOriginEndpoint(), ack);
                scalingContext.removePendingScaleInRequest(scaleInCompleteMsg.getPrefix());
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
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(ack.getPrefix());
        if (pendingReq != null) {
            int receivedCount = pendingReq.incrementAndGetReceivedCount();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleInCompleteAck from a child. " +
                                "Sent Ack Count: %d, Received Ack Count: %d", getInstanceIdentifier(),
                        pendingReq.getSentCount(), receivedCount));
            }
            if (receivedCount == pendingReq.getSentCount()) {
                if (!pendingReq.isInitiatedLocally()) {
                    ScaleInCompleteAck ackToParent = new ScaleInCompleteAck(ack.getPrefix(),
                            pendingReq.getOriginComputation());
                    try {
                        SendUtility.sendControlMessage(pendingReq.getOriginCtrlEndpoint(), ackToParent);
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending out a ScaleInCompleteAck to " + pendingReq.getOriginCtrlEndpoint());
                    }
                } else { // initiated locally.
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Completed Scaling in for prefix : %s",
                                getInstanceIdentifier(), ack.getPrefix()));
                    }
                    // update hazelcast
                    //try {
                        /*
                        IMap<String, SketchLocation> prefMap = getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                        prefMap.put(ack.getPrefix(), new SketchLocation(getInstanceIdentifier(), getCtrlEndpoint(),
                                SketchLocation.MODE_SCALE_IN));
                                */
                    IQueue<Integer> scaleMonitorQueue = getHzInstance().getQueue("scaling-monitor");
                    scaleMonitorQueue.add(-1);
                    //} catch (GranulesConfigurationException e) {
                    //    logger.error("Error publishing to Hazelcast.", e);
                    //}
                }
                scalingContext.removePendingScaleInRequest(ack.getPrefix());
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
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(prefix);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Received ScaleInActivateReq for prefix: %s", getInstanceIdentifier(),
                    prefix));
        }
        for (String lockedPrefix : pendingReq.getSentOutRequests().keySet()) {
            // disable pass-through
            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
            monitoredPrefix.setIsPassThroughTraffic(false);
            FullQualifiedComputationAddr reqInfo = pendingReq.getSentOutRequests().get(lockedPrefix);

            try {
                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(prefix, reqInfo.getComputationId(),
                        monitoredPrefix.getLastMessageSent(), monitoredPrefix.getLastGeoHashSent(), lockedPrefix.length(),
                        activationReq.getOriginNodeOfScalingOperation(),
                        activationReq.getOriginComputationOfScalingOperation());
                SendUtility.sendControlMessage(reqInfo.getCtrlEndpointAddr(), scaleInActivateReq);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Propagating ScaleInActivateReq to children. " +
                                    "Parent prefix: %s, Child prefix: %s, Last message processed: %d",
                            getInstanceIdentifier(), prefix, lockedPrefix, monitoredPrefix.getLastMessageSent()));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending ScaleInActivationRequest to " + reqInfo.getCtrlEndpointAddr(), e);
            }
        }
        for (String localPrefix : pendingReq.getLocallyProcessedPrefixes()) {
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

    public abstract byte[] split(String prefix);

    public abstract void merge(String prefix, byte[] serializedSketch);

    public void emit(String streamId, GeoHashIndexedRecord message) throws StreamingDatasetException {
        writeToStream(streamId, message);
    }

    public void releaseMutex(){
        mutex.release();
    }

    public boolean tryAcquireMutex(){
        return mutex.tryAcquire();
    }

    public HazelcastInstance getHzInstance() {
        if (hzInstance == null) {
            synchronized (this) {
                if (hzInstance == null) {
                    try {
                        hzInstance = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance();
                    } catch (HazelcastException e) {
                        logger.error("Error retrieving HzInstance.", e);
                    }
                }
            }
        }
        return hzInstance;
    }
}




