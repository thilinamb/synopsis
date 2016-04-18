package neptune.geospatial.core.computations;


import com.hazelcast.core.HazelcastInstance;
import ds.funnel.data.format.FormatReader;
import ds.funnel.data.format.FormatWriter;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.dataset.DatasetException;
import ds.granules.dataset.StreamEvent;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import ds.granules.streaming.core.partition.scheme.SendToAllPartitioner;
import neptune.geospatial.core.computations.scalingctxt.*;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.msg.scalein.ScaleInActivateReq;
import neptune.geospatial.core.protocol.msg.scalein.ScaleInLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutRequest;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.core.protocol.processors.StateTransferMsgProcessor;
import neptune.geospatial.core.protocol.processors.scalein.*;
import neptune.geospatial.core.protocol.processors.scalout.*;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.ft.*;
import neptune.geospatial.ft.protocol.CheckpointAck;
import neptune.geospatial.ft.protocol.CheckpointAckProcessor;
import neptune.geospatial.ft.protocol.StateReplLvlIncreaseMsgProcessor;
import neptune.geospatial.ft.zk.MembershipChangeListener;
import neptune.geospatial.ft.zk.MembershipTracker;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import neptune.geospatial.util.Mutex;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;
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
public abstract class AbstractGeoSpatialStreamProcessor extends StreamProcessor implements FaultTolerantStreamBase,
        MembershipChangeListener {

    private Logger logger = Logger.getLogger(AbstractGeoSpatialStreamProcessor.class.getName());
    private static final String OUTGOING_STREAM_BASE_ID = "out-going";
    public static final int MAX_CHARACTER_DEPTH = 4;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;

    private AtomicInteger outGoingStreamIdSeqGenerator = new AtomicInteger(100);
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger messageSize = new AtomicInteger(-1);
    private AtomicLong tsLastUpdated = new AtomicLong(0);

    private ScalingContext scalingContext;
    private String ctrlEndpoint;

    // mutex to ensure only a single scale in/out operations takes place at a given time
    private final Mutex mutex = new Mutex();

    // Hazelcast + prefix tree
    private HazelcastInstance hzInstance;

    // protocol processors
    private Map<Integer, ProtocolProcessor> protocolProcessors = new HashMap<>();

    // fault tolerance related attributes
    private boolean faultToleranceEnabled;
    private Map<String, List<BackupTopicInfo>> topicLocations = new HashMap<>();
    private List<TopicInfo> replicationStreamTopics;

    private Map<Long, PendingCheckpoint> pendingCheckpoints = new HashMap<>();

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

    /**
     * Returns an estimation of the memory consumed by the sketch for a given prefix
     *
     * @param prefix Prefix String
     * @return estimation of consumed memory
     */
    public double getMemoryConsumptionForPrefix(String prefix) {
        throw new UnsupportedOperationException("getMemoryConsumptionForPrefix is not implemented in " +
                "AbstractGeoSpatialStreamProcessor class.");
    }

    /**
     * Returns an estimation of memory consumption by all prefixes
     *
     * @return estimated memory consumption
     */
    public double getMemoryConsumptionForAllPrefixes() {
        throw new UnsupportedOperationException("getMemoryConsumptionForAllPrefixes is not implemented in " +
                "AbstractGeoSpatialStreamProcessor class.");
    }

    /**
     * Returns the state change since last invocation of this method.
     * If this is invoked for the first time, returns the base version.
     *
     * @return Serialized state change
     */
    public byte[] getSketchDiff() {
        throw new UnsupportedOperationException("getSketchDiff is not implemented in " +
                "AbstractGeoSpatialStreamProcessor class.");
    }

    /**
     * Repopulate the sketch using serialized state changes stored in disk
     *
     * @param baseDirPath Path to the directory where the serialized state changes are stored
     */
    public void populateSketch(String baseDirPath) {
        throw new UnsupportedOperationException("populateSketch is not implemented in " +
                "AbstractGeoSpatialStreamProcessor class.");
    }

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        if (!initialized.get()) {
            init();
        }
        if (messageSize.get() == -1) {
            messageSize.set(getMessageSize(streamEvent));
        }
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        // this a dummy message sent to activate the computation after scaling out.
        if (geoHashIndexedRecord.getMessageIdentifier() == -1) {
            return;
        }
        // preprocess each message
        long checkpointId = geoHashIndexedRecord.getCheckpointId();
        if (checkpointId <= 0 && preprocess(geoHashIndexedRecord)) {
            // perform the business logic: do this selectively. Send through the traffic we don't process.
            process(geoHashIndexedRecord);
        }
        if (faultToleranceEnabled) {
            if (checkpointId > 0) {
                // send out a dummy state replication message for now
                byte[] serializedState = new byte[100];
                new Random().nextBytes(serializedState);
                StateReplicationMessage stateReplicationMessage = new StateReplicationMessage(
                        checkpointId, (byte) 1, serializedState, getInstanceIdentifier(),
                        ctrlEndpoint);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] state was replicated.", getInstanceIdentifier()));
                }
                GeoHashIndexedRecord recordToChildren = new GeoHashIndexedRecord(
                        checkpointId, getInstanceIdentifier(), this.ctrlEndpoint);
                List<String> outgoingStreams = scalingContext.getOutgoingStreams();
                // keep track of the pending checkpoint
                PendingCheckpoint pendingCheckpoint = new PendingCheckpoint(checkpointId,
                        replicationStreamTopics.size(), outgoingStreams.size(), geoHashIndexedRecord.getParentId(),
                        geoHashIndexedRecord.getParentEndpoint());
                pendingCheckpoints.put(checkpointId, pendingCheckpoint);
                // write to replication streams
                writeToStream(Constants.Streams.STATE_REPLICA_STREAM, stateReplicationMessage);
                // propagate the request to child nodes
                for (String outgoingStream : outgoingStreams) {
                    writeToStream(outgoingStream, recordToChildren);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Propagated checkpoint trigger to child nodes. " +
                                    "Checkpoint Id: %d Number of children: %d", getInstanceIdentifier(),
                            checkpointId, outgoingStreams.size()));
                }
            }
        }
    }

    private synchronized void init() {
        if (!initialized.get()) {
            try {
                // register with the resource to enable monitoring
                initializeProtocolProcessors();
                this.scalingContext = new ScalingContext(getInstanceIdentifier());
                ManagedResource resource = ManagedResource.getInstance();
                resource.registerStreamProcessor(this);
                this.faultToleranceEnabled = resource.isFaultToleranceEnabled();
                this.ctrlEndpoint = RivuletUtil.getCtrlEndpoint();
                initialized.set(true);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Initialized. Message Size: %d", getInstanceIdentifier(),
                            messageSize.get()));
                }
            } catch (NIException e) {
                logger.error("Error retrieving the resource instance.", e);
            } catch (GranulesConfigurationException e) {
                logger.error("Error retrieving control endpoint.", e);
            }
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
                    logger.debug("Waiting until a secondary is swapped with the primary.");
                    try {
                        this.wait();
                    } catch (InterruptedException ignore) {

                    }
                    logger.debug("Resuming message processing after the swap is completed.");
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
        protocolProcessors.put(ProtocolTypes.STATE_REPL_LEVEL_INCREASE, new StateReplLvlIncreaseMsgProcessor());
        protocolProcessors.put(ProtocolTypes.CHECKPOINT_ACK, new CheckpointAckProcessor());
    }

    /**
     * deploy outgoing streams for state replication
     *
     * @param topics Stream ids of chosen replica locations
     */
    public void deployStateReplicationStreams(Topic[] topics) {
        String fqStreamId = getStreamIdentifier(Constants.Streams.STATE_REPLICA_STREAM);
        try {
            for (Topic topic : topics) {
                this.streamDataset.addOutputStream(topic);
                String streamType = StateReplicationMessage.class.getName();
                outGoingStreamTypes.put(fqStreamId, streamType);
                outGoingStreamTypes.put(topic.toString(), streamType);
            }

            StreamDisseminationMetadata streamDisseminationMetadata = new StreamDisseminationMetadata(new SendToAllPartitioner(), topics);
            if (metadataRegistry.containsKey(Constants.Streams.STATE_REPLICA_STREAM)) {
                metadataRegistry.get(Constants.Streams.STATE_REPLICA_STREAM).add(streamDisseminationMetadata);
            } else {
                List<StreamDisseminationMetadata> streamDisseminationMetadataElems = new ArrayList<>();
                streamDisseminationMetadataElems.add(streamDisseminationMetadata);
                metadataRegistry.put(Constants.Streams.STATE_REPLICA_STREAM, streamDisseminationMetadataElems);
            }
        } catch (DatasetException e) {
            e.printStackTrace();
        }
    }

    public void registerDefaultTopic(Topic topic) throws StreamingDatasetException {
        try {
            this.getDefaultStreamDataset().addInputStream(topic, this.getInstanceIdentifier());
            // add the incoming stream type at the destination.
            this.incomingStreamTypes.put(topic.toString(), GeoHashIndexedRecord.class.getName());
            this.identifierMap.put(Integer.parseInt(topic.toString()), Constants.Streams.NOAA_DATA_STREAM);
        } catch (DatasetException e) {
            logger.error(e.getMessage(), e);
            throw new StreamingDatasetException(e.getMessage(), e);
        }
    }

    /**
     * Returns the default incoming geo-spatial stream topic.
     * Used by the deployer initially to figure out the incoming streams
     * to create the replication topic tree in zk.
     *
     * @return Default incoming geo spatial stream topic
     */
    public Topic getDefaultGeoSpatialStream() {
        List<Topic> topics = new ArrayList<>();
        topics.addAll(this.getDefaultStreamDataset().getInputStreams());
        return topics.get(0);
    }

    @Override
    protected void deserializeMemberVariables(FormatReader formatReader) {
        try {
            if (ManagedResource.getInstance().isFaultToleranceEnabled()) {
                // we need to make sure that the messages from deployer about replication level increasing
                // can reach the computation
                if (!initialized.get()) {
                    init();
                }
                int replicationElementCount = formatReader.readInt();
                this.replicationStreamTopics = new ArrayList<>();
                for (int i = 0; i < replicationElementCount; i++) {
                    TopicInfo topicInfo = new TopicInfo();
                    topicInfo.unmarshall(formatReader);
                    this.replicationStreamTopics.add(topicInfo);
                }
                // Hack: since Granules does not reinitialize operators after deploying
                // we need a way to read the backup topics from the zk tree just after the deployment.
                // doing it lazily is expensive.
                this.topicLocations = this.populateBackupTopicMap(getInstanceIdentifier(), metadataRegistry);
                MembershipTracker.getInstance().registerListener(this);
            }
        } catch (NIException e) {
            logger.error("Error acquiring the Resource instance.", e);
        } catch (CommunicationsException e) {
            logger.error("Error registering for cluster memberhsip changes.", e);
        }
    }

    public void populateBackupTopicsPerStream(String stream) throws ScalingException {
        if (faultToleranceEnabled) {
            try {
                processBackupTopicPerStream(ZooKeeperAgent.getInstance().getZooKeeperInstance(),
                        getInstanceIdentifier(), stream, metadataRegistry, topicLocations);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Backup topics are populated for the new outgoing stream: %s",
                            getInstanceIdentifier(), stream));
                }
            } catch (KeeperException | InterruptedException | CommunicationsException e) {
                throw new ScalingException("Error updating backup topics for newly deployed child node.", e);
            }
        }
    }

    public void setReplicationStreamTopics(List<TopicInfo> replicationStreamTopics) {
        this.replicationStreamTopics = replicationStreamTopics;
    }

    @Override
    protected void serializedMemberVariables(FormatWriter formatWriter) {
        if (this.replicationStreamTopics != null) {
            formatWriter.writeInt(this.replicationStreamTopics.size());
            for (TopicInfo topicInfo : this.replicationStreamTopics) {
                topicInfo.marshall(formatWriter);
            }
        }
    }

    @Override
    public void membershipChanged(List<String> lostMembers) {
        synchronized (this) {
            boolean updateTopicLocations = false;
            List<TopicInfo> currentStateReplicationTopics = new ArrayList<>();
            for (String lostMember : lostMembers) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Processing the lost node: %s", getInstanceIdentifier(), lostMember));
                }
                // check if a node that hosts an out-going topic has left the cluster
                if (topicLocations.containsKey(lostMember)) {
                    // get the list of all topics that was running on the lost node
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Current node is affected by the lost node. Lost node: %s, " +
                                        "Number of affected topics: %d", getInstanceIdentifier(), lostMember,
                                topicLocations.get(lostMember).size()));
                    }
                    switchToSecondary(topicLocations, lostMember, getInstanceIdentifier(), metadataRegistry);
                    updateTopicLocations = true;
                }
                for (TopicInfo stateReplicaProcessor : replicationStreamTopics) {
                    if (stateReplicaProcessor.getResourceEndpoint().equals(lostMember)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] A state replication processor is affected. " +
                                    "State Replication Topic: %s", getInstanceIdentifier(), stateReplicaProcessor.getTopic()));
                        }
                        List<StreamDisseminationMetadata> metadataList = metadataRegistry.get(Constants.Streams.STATE_REPLICA_STREAM);
                        // one of the replicas has failed. Increase the replica level.
                        if (metadataList != null) {
                            for (StreamDisseminationMetadata metadata : metadataList) {
                                List<Topic> validTopics = new ArrayList<>();
                                for (Topic replicationTopic : metadata.topics) {
                                    if (!replicationTopic.equals(stateReplicaProcessor.getTopic())) {
                                        validTopics.add(replicationTopic);
                                    } else {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug(String.format("[%s] " +
                                                            "Replication topic on the lost node was removed. Topic: %s",
                                                    getInstanceIdentifier(), replicationTopic.toString()));
                                        }
                                    }
                                }
                                if (validTopics.size() < metadata.topics.length) {
                                    metadata.topics = validTopics.toArray(new Topic[validTopics.size()]);
                                }
                            }
                        }
                    } else {
                        currentStateReplicationTopics.add(stateReplicaProcessor);
                    }
                }
            }
            // update the current replication topics
            replicationStreamTopics = currentStateReplicationTopics;
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Current replication processor count: %d", getInstanceIdentifier(),
                        replicationStreamTopics.size()));
            }
            // it is required to repopulate the backup nodes list
            if (updateTopicLocations) {
                populateBackupTopicMap(this.getInstanceIdentifier(), metadataRegistry);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] BackupTopicMap is updated.", getInstanceIdentifier()));
                }
            }
            this.notifyAll();
        }
    }

    public void addNewStateReplicationTopic(Topic topic, String newLocation) {
        TopicInfo topicInfo = new TopicInfo(topic, newLocation);
        if (replicationStreamTopics.contains(topicInfo)) {
            logger.error(String.format("[%s] Duplicate state replication topic detected. Topic: %s, Location: %s",
                    getInstanceIdentifier(), topic, newLocation));
            return;
        }
        replicationStreamTopics.add(topicInfo);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Added new stare replication topic. Topic: %s, New Location: %s",
                    getInstanceIdentifier(), topic, newLocation));
        }
        // update the meta data registry
        List<StreamDisseminationMetadata> metadataList = metadataRegistry.get(Constants.Streams.STATE_REPLICA_STREAM);
        if (metadataList != null) {
            for (StreamDisseminationMetadata metadata : metadataList) {
                List<Topic> replicationTopics = new ArrayList<>();
                replicationTopics.addAll(Arrays.asList(metadata.topics));
                if (!replicationTopics.contains(topic)) {
                    replicationTopics.add(topic);
                    metadata.topics = replicationTopics.toArray(new Topic[replicationTopics.size()]);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Updated stream dissemination " +
                                "metadata with new replication topic. Topic: %s", getInstanceIdentifier(), topic));
                    }
                } else {
                    logger.error(String.format("[%s] Duplicate state replication topic detected. Topic: %s, Location: %s",
                            getInstanceIdentifier(), topic, newLocation));
                }
            }
        }
    }

    public void handleAckStatePersistence(CheckpointAck ack) {
        long checkpointId = ack.getCheckpointId();
        PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);
        if (pendingCheckpoint != null) {
            int pendingCount;
            if (ack.isFromReplicator()) {
                pendingCount = pendingCheckpoint.ackFromStateReplicationProcessor();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received an ack from state replicator. " +
                                    "Checkpoint id: %d, State Replicator endpoint: %s, Pending State Replication acks: %d, " +
                                    "Pending Child acks: %d", getInstanceIdentifier(),
                            checkpointId, ack.getOriginEndpoint(), pendingCheckpoint.getStateReplicationAcks(),
                            pendingCheckpoint.getChildAcks()));
                }
            } else {
                pendingCount = pendingCheckpoint.ackFromChild();
                logger.debug(String.format("[%s] Received an ack from a child. " +
                                "Checkpoint id: %d, Child endpoint: %s, Pending State Replication acks: %d, " +
                                "Pending Child acks: %d", getInstanceIdentifier(),
                        checkpointId, ack.getOriginEndpoint(), pendingCheckpoint.getStateReplicationAcks(),
                        pendingCheckpoint.getChildAcks()));
            }
            if (pendingCount == 0 && pendingCheckpoint.isCheckpointCompleted()) {
                CheckpointAck ackToParent = new CheckpointAck(CheckpointAck.ACK_FROM_CHILD,
                        checkpointId, pendingCheckpoint.getParentCompId());
                try {
                    SendUtility.sendControlMessage(pendingCheckpoint.getParentCompEndpoint(), ackToParent);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Received all acks. Acknowledging parent. " +
                                "Checkpoint id: %d", getInstanceIdentifier(), checkpointId));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error(String.format("[%s] Error sending checkpoint ack to parent. " +
                                    "Checkpoint id: %d, Parent endpoint: %s", getInstanceIdentifier(), checkpointId,
                            pendingCheckpoint.getParentCompEndpoint()));
                }
            }
        } else {
            logger.warn(String.format("[%s] Invalid AckStatePersistence. Checkpoing Id: %d", getInstanceIdentifier(),
                    checkpointId));
        }
    }
}
