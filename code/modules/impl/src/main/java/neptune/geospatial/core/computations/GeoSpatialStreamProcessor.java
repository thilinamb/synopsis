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

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stream processor specialized for geo-spatial data processing.
 * Specific computations may be implement by extending this
 * abstract class.
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
        private long tsLastUpdated;

        public MonitoredPrefix(String prefix, String streamType) {
            this.prefix = prefix;
            this.streamType = streamType;
        }

        @Override
        public int compareTo(MonitoredPrefix o) {
            return (int) (this.messageRate - o.messageRate);
        }
    }

    private class PendingScaleOutRequests {
        private String prefix;
        private String streamId;

        public PendingScaleOutRequests(String prefix, String streamId) {
            this.prefix = prefix;
            this.streamId = streamId;
        }
    }

    private Logger logger = Logger.getLogger(GeoSpatialStreamProcessor.class.getName());
    public static final String OUTGOING_STREAM_BASE_ID = "out-going";
    private static final String GEO_HASH_CHAR_SET = "0123456789bcdefghjkmnpqrstuvwxyz";
    public static final int GEO_HASH_LEN_IN_CHARS = 32;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;

    private AtomicBoolean initialized = new AtomicBoolean(false);
    private Set<MonitoredPrefix> monitoredPrefixes = new TreeSet<>();
    private Map<String, MonitoredPrefix> monitoredPrefixMap = new HashMap<>();
    private Map<String, String> outGoingStreams = new HashMap<>();
    private Map<String, PendingScaleOutRequests> pendingScaleOutRequests = new HashMap<>();

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        if (!initialized.get()) {
            try {
                ManagedResource.getInstance().registerStreamProcessor(this);
                initialized.set(true);
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
        boolean processLocally = true;
        if (outGoingStreams.containsKey(prefix)) {
            writeToStream(outGoingStreams.get(prefix), record);
            processLocally = false;
        }
        return processLocally;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }

    public void handleTriggerScaleAck(TriggerScaleAck ack) {
        if (pendingScaleOutRequests.containsKey(ack.getInResponseTo())) {
            PendingScaleOutRequests pendingReq = pendingScaleOutRequests.remove(ack.getInResponseTo());
            outGoingStreams.put(pendingReq.prefix, pendingReq.streamId);
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
        if (monitoredPrefix.tsLastUpdated == 0) {
            monitoredPrefix.tsLastUpdated = timeNow;
        } else if ((timeNow - monitoredPrefix.tsLastUpdated) > INPUT_RATE_UPDATE_INTERVAL) {
            double timeElapsed = (timeNow - monitoredPrefix.tsLastUpdated) * 1.0;
            DecimalFormat dFormat = new DecimalFormat();
            monitoredPrefix.messageRate = monitoredPrefix.messageCount * 1000.0 / timeElapsed;
            monitoredPrefix.messageCount = 0;
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Prefix: %s, Message Rate: %.3f", prefix, monitoredPrefix.messageRate));
            }
            monitoredPrefix.tsLastUpdated = timeNow;
        }
    }

    private String getPrefix(GeoHashIndexedRecord record) {
        return record.getGeoHash().substring(0, record.getPrefixLength() + 1);
    }

    private int getIndexForSubPrefix(String geohash, int prefixLength) {
        return GEO_HASH_CHAR_SET.indexOf(geohash.charAt(prefixLength));
    }

    public long getBacklogLength() {
        return streamDataset.getQueueLengthInBytes();
    }

    /**
     * Resource recommends scaling out for one or more prefixes.
     */
    public void recommendScaleOut(double excess) {
        List<String> prefixesForScalingOut = new ArrayList<>();
        double cumulSumOfPrefixes = 0;
        synchronized (this) {
            Iterator<MonitoredPrefix> itr = monitoredPrefixes.iterator();
            while (itr.hasNext() && cumulSumOfPrefixes < excess) {
                MonitoredPrefix monitoredPrefix = itr.next();
                prefixesForScalingOut.add(monitoredPrefix.prefix);
                cumulSumOfPrefixes += monitoredPrefix.messageRate;
            }
        }
        for (String prefix : prefixesForScalingOut) {
            String streamType = monitoredPrefixMap.get(prefix).streamType;
            initiateScaleOut(prefix, streamType);
        }
    }

    public void initiateScaleOut(String prefix, String streamType) {
        try {
            GeoHashPartitioner partitioner = new GeoHashPartitioner();
            String outGoingStreamId = getNewStreamIdentifier(prefix);
            declareStream(outGoingStreamId, streamType);
            // initialize the meta-data
            Topic[] topics = deployStream(outGoingStreamId, 1, partitioner);

            TriggerScale triggerMessage = new TriggerScale(getInstanceIdentifier(), outGoingStreamId,
                    topics[0].toString(), streamType);
            pendingScaleOutRequests.put(triggerMessage.getMessageId(), new PendingScaleOutRequests(prefix, outGoingStreamId));
            ManagedResource.getInstance().sendToDeployer(triggerMessage);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Sent a trigger scale message to deployer.",
                        getInstanceIdentifier()));
            }
        } catch (StreamingDatasetException | StreamingGraphConfigurationException e) {
            logger.error("Error creating new stream from the current computation.", e);
        } catch (NIException e) {
            logger.error("Error sending a trigger scale message to the deployer. ", e);
        }
    }

    private String getNewStreamIdentifier(String prefix) {
        return getStreamIdentifier(OUTGOING_STREAM_BASE_ID + "-" + prefix);
    }
}




