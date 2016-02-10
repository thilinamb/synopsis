package neptune.geospatial.core.computations;


import ds.funnel.topic.Topic;
import ds.granules.dataset.StreamEvent;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.protocol.msg.TriggerScale;
import neptune.geospatial.core.protocol.msg.TriggerScaleAck;
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

    private class MonitoredPrefix implements Comparable<MonitoredPrefix> {
        private String prefix;
        private String streamType;
        private long messageCount;
        private double messageRate;

        public MonitoredPrefix(String prefix, String streamType) {
            this.prefix = prefix;
            this.streamType = streamType;
        }

        @Override
        public int compareTo(MonitoredPrefix o) {
            if (this.messageRate == o.messageRate) {
                return this.prefix.compareTo(o.prefix);
            } else {
                return (-1) * (int) (this.messageRate - o.messageRate);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MonitoredPrefix that = (MonitoredPrefix) o;

            if (!prefix.equals(that.prefix)) return false;
            if (!streamType.equals(that.streamType)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = prefix.hashCode();
            result = 31 * result + streamType.hashCode();
            return result;
        }
    }

    private class PendingScaleOutRequests {
        private List<String> prefixes;
        private String streamId;

        public PendingScaleOutRequests(List<String> prefixes, String streamId) {
            this.prefixes = prefixes;
            this.streamId = streamId;
        }
    }

    private Logger logger = Logger.getLogger(GeoSpatialStreamProcessor.class.getName());
    public static final String OUTGOING_STREAM_BASE_ID = "out-going";
    private static final String GEO_HASH_CHAR_SET = "0123456789bcdefghjkmnpqrstuvwxyz";
    public static final int GEO_HASH_LEN_IN_CHARS = 32;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;
    private AtomicLong tsLastUpdated = new AtomicLong(0);
    private AtomicInteger outGoingStreamIdSeqGenerator = new AtomicInteger(100);

    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicInteger messageSize = new AtomicInteger(-1);
    private Set<MonitoredPrefix> monitoredPrefixes = new TreeSet<>();
    private Map<String, MonitoredPrefix> monitoredPrefixMap = new HashMap<>();
    private Map<String, String> outGoingStreams = new HashMap<>();
    private Map<String, PendingScaleOutRequests> pendingScaleOutRequests = new HashMap<>();

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
        updateIncomingRatesForSubPrefixes(record);
        String prefix = getPrefix(record);
        boolean processLocally;
        // if there is an outgoing stream, then this should be sent to a child node.
        synchronized (this) {
            processLocally = !outGoingStreams.containsKey(prefix);
        }
        if (!processLocally) {
            record.setPrefixLength(record.getPrefixLength() + 1);
            // send to the child node
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Forwarding Message. Prefix: %s, Outgoing Stream: %s",
                        getInstanceIdentifier(), prefix, outGoingStreams.get(prefix)));
            }
            writeToStream(outGoingStreams.get(prefix), record);
        }
        return processLocally;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }

    public synchronized void handleTriggerScaleAck(TriggerScaleAck ack) {
        if (pendingScaleOutRequests.containsKey(ack.getInResponseTo())) {
            PendingScaleOutRequests pendingReq = pendingScaleOutRequests.remove(ack.getInResponseTo());
            for (String prefix : pendingReq.prefixes) {
                outGoingStreams.put(prefix, pendingReq.streamId);
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
                    ManagedResource.getInstance().scaleOutComplete(this.getInstanceIdentifier());
                } catch (NIException ignore) {

                }
            }
        } else {
            logger.warn("Invalid trigger ack for the prefix. Request Id: " + ack.getInResponseTo());
        }
    }

    private synchronized void updateIncomingRatesForSubPrefixes(GeoHashIndexedRecord record) {
        String prefix = getPrefix(record);
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
        for (String monitoredPrefStr : monitoredPrefixMap.keySet()) {
            monitoredPrefix = monitoredPrefixMap.get(monitoredPrefStr);
            if (tsLastUpdated.get() == 0) {
                tsLastUpdated.set(timeNow);
            } else if ((timeNow - tsLastUpdated.get()) > INPUT_RATE_UPDATE_INTERVAL) {
                double timeElapsed = (timeNow - tsLastUpdated.get()) * 1.0;
                DecimalFormat dFormat = new DecimalFormat();
                monitoredPrefix.messageRate = monitoredPrefix.messageCount * 1000.0 / timeElapsed;
                monitoredPrefix.messageCount = 0;
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("[%s] Prefix: %s, Message Rate: %.3f", getInstanceIdentifier(),
                            prefix, monitoredPrefix.messageRate));
                }
                tsLastUpdated.set(timeNow);
            }
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
    public synchronized void recommendScaleOut(double excess) {
        List<String> prefixesForScalingOut = new ArrayList<>();
        double cumulSumOfPrefixes = 0;
        Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
        while (itr.hasNext() && cumulSumOfPrefixes < excess) {
            MonitoredPrefix monitoredPrefix = itr.next();
            if (!outGoingStreams.containsKey(monitoredPrefix.prefix) && monitoredPrefix.messageRate > 0) {
                prefixesForScalingOut.add(monitoredPrefix.prefix);
                cumulSumOfPrefixes += monitoredPrefix.messageRate * 2; // let's consider the number of messages accumulated
            }
            // over 2s.
        }
        if (logger.isDebugEnabled()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String prefix : prefixesForScalingOut) {
                stringBuilder.append(prefix).append("(").append(monitoredPrefixMap.get(prefix).messageRate).append("), ");
            }
            logger.debug(String.format("[%s] Scale Out recommendation. Excess: %.3f, Chosen Prefixes: %s",
                    getInstanceIdentifier(), excess, stringBuilder.toString()));
        }
        if (!prefixesForScalingOut.isEmpty()) {
            // We assume we use the same message type throughout the graph.
            String streamType = monitoredPrefixMap.get(prefixesForScalingOut.get(0)).streamType;
            initiateScaleOut(prefixesForScalingOut, streamType);
        } else {
            try {
                ManagedResource.getInstance().scaleOutComplete(getInstanceIdentifier());
            } catch (NIException ignore) {

            }
        }
    }

    public void initiateScaleOut(List<String> prefix, String streamType) {
        try {
            GeoHashPartitioner partitioner = new GeoHashPartitioner();
            String outGoingStreamId = getNewStreamIdentifier();
            declareStream(outGoingStreamId, streamType);
            // initialize the meta-data
            Topic[] topics = deployStream(outGoingStreamId, 1, partitioner);

            TriggerScale triggerMessage = new TriggerScale(getInstanceIdentifier(), outGoingStreamId,
                    topics[0].toString(), streamType);
            pendingScaleOutRequests.put(triggerMessage.getMessageId(), new PendingScaleOutRequests(prefix, outGoingStreamId));
            ManagedResource.getInstance().sendToDeployer(triggerMessage);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Sent a trigger scale message to deployer for the prefix: %s.",
                        getInstanceIdentifier(), prefix));
            }
        } catch (StreamingDatasetException | StreamingGraphConfigurationException e) {
            logger.error("Error creating new stream from the current computation.", e);
        } catch (NIException e) {
            logger.error("Error sending a trigger scale message to the deployer. ", e);
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
}




