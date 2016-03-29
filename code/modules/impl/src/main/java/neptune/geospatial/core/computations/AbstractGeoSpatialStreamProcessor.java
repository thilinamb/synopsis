package neptune.geospatial.core.computations;


import com.hazelcast.core.HazelcastInstance;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.dataset.StreamEvent;
import ds.granules.exception.CommunicationsException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.computations.scalingctxt.*;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.scalein.ScaleInActivateReq;
import neptune.geospatial.core.protocol.msg.scalein.ScaleInLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutRequest;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.processors.*;
import neptune.geospatial.core.protocol.processors.scalein.*;
import neptune.geospatial.core.protocol.processors.scalout.*;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import neptune.geospatial.util.Mutex;
import org.apache.log4j.Logger;

import java.io.IOException;
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
public abstract class AbstractGeoSpatialStreamProcessor extends StreamProcessor {

    private Logger logger = Logger.getLogger(AbstractGeoSpatialStreamProcessor.class.getName());
    private static final String OUTGOING_STREAM_BASE_ID = "out-going";
    public static final int MAX_CHARACTER_DEPTH = 4;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;

    private AtomicInteger outGoingStreamIdSeqGenerator = new AtomicInteger(100);
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger messageSize = new AtomicInteger(-1);
    private AtomicLong tsLastUpdated = new AtomicLong(0);

    private ScalingContext scalingContext;

    // mutex to ensure only a single scale in/out operations takes place at a given time
    private final Mutex mutex = new Mutex();

    // Hazelcast + prefix tree
    private HazelcastInstance hzInstance;

    // protocol processors
    private Map<Integer, ProtocolProcessor> protocolProcessors = new HashMap<>();


    /**
     * Implement the specific business logic to process each
     * <code>GeohashIndexedRecord</code> message.
     *
     * @param event <code>GeoHashIndexedRecord</code> element
     */
    protected abstract void process(GeoHashIndexedRecord event);

    /**
     * Return the state for the given prefix
     *
     * @param prefix Geohash Prefix
     * @return serialized state of the prefix
     */
    public abstract byte[] split(String prefix);

    /**
     * Merge the state of the provided prefix with the current prefix
     *
     * @param prefix           Prefix
     * @param serializedSketch Serialized state corresponding to the prefix
     */
    public abstract void merge(String prefix, byte[] serializedSketch);

    /**
     * Invoked when Scale out protocol is initiated.
     * Can be used to track scaling out acitivity along with {@code onSuccessfulScaleOut}.
     * Overriding this method is optional.
     */
    public void onStartOfScaleOut() {
    }

    /**
     * Invoked when Scale in protocol is initiated.
     * Can be used to track scaling in acitivity along with {@code onSuccessfulScaleIn}.
     * Overriding this method is optional.
     */
    public void onStartOfScaleIn() {
    }

    /**
     * Invoked after a successful completion of a scale out operation.
     * Can be used to track dynamic scaling activity.
     * Overriding this method is optional.
     *
     * @param prefixes List of prefixes that were scaled out.
     */
    public void onSuccessfulScaleOut(List<String> prefixes) {
    }

    /**
     * Invoked after a successful completion of a scale in operation.
     * Similar to {@code onSuccessfulScaleOut}, this method can also be used to track
     * dynamic scaling activity.
     * Overriding this method is optional.
     *
     * @param prefixes List of prefixes that were scaled in.
     */
    public void onSuccessfulScaleIn(List<String> prefixes) {
    }

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        if (!initialized.get()) {
            try {
                // register with the resource to enable monitoring
                initializeProtocolProcessors();
                messageSize.set(getMessageSize(streamEvent));
                this.scalingContext = new ScalingContext(getInstanceIdentifier());
                ManagedResource.getInstance().registerStreamProcessor(this);
                initialized.set(true);
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
     * Preprocess every record to extract meta-data such as triggering
     * scale out operations. This is prior to performing actual processing
     * on a message.
     *
     * @param record <code>GeoHashIndexedRecord</code> element
     * @return Whether to process the record locally(true) or not(false).
     */
    private boolean preprocess(GeoHashIndexedRecord record) throws StreamingDatasetException {
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

    public String getPrefix(String geohash, int prefixLength) {
        return geohash.substring(0, prefixLength + 1);
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

    public synchronized void processCtrlMessage(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        ProtocolProcessor protocolProcessor = protocolProcessors.get(type);
        if (protocolProcessor != null) {
            protocolProcessor.process(ctrlMsg, scalingContext, this);
        } else {
            logger.error(String.format("[%s] Unsupported protocol message. Type: %d, Class: %s",
                    getInstanceIdentifier(), type, ctrlMsg.getClass().getName()));
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
            onStartOfScaleOut();
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
            onStartOfScaleIn();
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

    public void emit(String streamId, GeoHashIndexedRecord message) throws StreamingDatasetException {
        writeToStream(streamId, message);
    }

    public void releaseMutex() {
        mutex.release();
    }

    public boolean tryAcquireMutex() {
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

    private void initializeProtocolProcessors() {
        protocolProcessors.put(ProtocolTypes.SCALE_OUT_RESP, new ScaleOutResponseProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_OUT_LOCK_REQ, new ScaleOutLockRequestProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_OUT_LOCK_RESP, new ScaleOutLockResponseProcessor());
        protocolProcessors.put(ProtocolTypes.STATE_TRANSFER_COMPLETE_ACK, new StateTransferCompleteAckProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_OUT_COMPLETE, new ScaleOutCompleteMsgProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_OUT_COMPLETE_ACK, new ScaleOutCompleteAckProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_IN_LOCK_REQ, new ScaleInLockReqProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_IN_LOCK_RESP, new ScaleInLockResponseProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_IN_ACTIVATION_REQ, new ScaleInActivateReqProcessor());
        protocolProcessors.put(ProtocolTypes.STATE_TRANSFER_MSG, new StateTransferMsgProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_IN_COMPLETE, new ScaleInCompleteMsgProcessor());
        protocolProcessors.put(ProtocolTypes.SCALE_IN_COMPLETE_ACK, new ScaleInCompleteAckProcessor());
    }
}
