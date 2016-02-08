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
import java.util.HashMap;
import java.util.Map;
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

    private class PendingScaleOutRequests {

        private String streamId;    // the new stream id

        public PendingScaleOutRequests(String streamId) {
            this.streamId = streamId;
        }
    }

    private Logger logger = Logger.getLogger(GeoSpatialStreamProcessor.class.getName());
    public static final String OUTGOING_STREAM = "out-going";
    private static final String GEO_HASH_CHAR_SET = "0123456789bcdefghjkmnpqrstuvwxyz";
    public static final int GEO_HASH_LEN_IN_CHARS = 32;
    private static final int INPUT_RATE_UPDATE_INTERVAL = 10 * 1000;

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private volatile int counter = 0;
    private AtomicBoolean scaleOutComplete = new AtomicBoolean(false);
    private AtomicBoolean scaledOut = new AtomicBoolean(false);
    private String outGoingStream;
    private Map<String, PendingScaleOutRequests> pendingScaleOutRequestsMap = new HashMap<>();
    private Map<String, long[]> messageCountsForSubPrefixesMap = new HashMap<>();
    private Map<String, double[]> messageRatesForSubPrefixesMap = new HashMap<>();
    private long lastUpdatedTime;

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
        counter++;
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        // preprocess each message
        if (preprocess(geoHashIndexedRecord)) {
            // perform the business logic: do this selectively. Send through the traffic we don't process.
            onEvent(geoHashIndexedRecord);
        }
    }

    /**
     * Implement the specific business logic to process each
     * <code>GeohashIndexedRecord</code> message.
     *
     * @param event <code>GeoHashIndexedRecord</code> element
     */
    protected abstract void onEvent(GeoHashIndexedRecord event);

    /**
     * Preprocess every record to extract meta-data such as triggering
     * scale out operations. This is prior to performing actual processing
     * on a message.
     *
     * @param record <code>GeoHashIndexedRecord</code> element
     */
    protected boolean preprocess(GeoHashIndexedRecord record) {
        updateIncomingRatesForSubPrefixes(record);
        boolean processLocally = true;
        // Scale out logic
        // this is a temporary trigger to test the scaling out.
        if (!scaledOut.get() && counter >= 1000000) {
            try {
                // FIXME: BEGIN - Need to do it only once
                this.outGoingStream = getStreamIdentifier(OUTGOING_STREAM);
                GeoHashPartitioner partitioner = new GeoHashPartitioner();
                String streamType = record.getClass().getName();
                declareStream(this.outGoingStream, streamType);
                // FIXME: END
                // initialize the meta-data
                Topic[] topics = deployStream(this.outGoingStream, 1, partitioner);

                TriggerScale triggerMessage = new TriggerScale(getInstanceIdentifier(), this.outGoingStream,
                        topics[0].toString(), streamType);
                ManagedResource.getInstance().sendToDeployer(triggerMessage);

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Sent a trigger scale message to deployer.",
                            getInstanceIdentifier()));
                }
                // this is just to control the scaling to one instance for testing.
                scaledOut.set(true);
                // FIXME: Increase the prefix length of the record if needed. May be inside the onEvent method.
            } catch (StreamingDatasetException | StreamingGraphConfigurationException e) {
                logger.error("Error creating new stream from the current computation.", e);
            } catch (NIException e) {
                logger.error("Error sending a trigger scale message to the deployer. ", e);
            }
        } else if (scaleOutComplete.get()) {
            try {
                writeToStream(outGoingStream, record);
                processLocally = false;
                //System.out.println("forwarding to the next child element.");
            } catch (StreamingDatasetException e) {
                e.printStackTrace();
            }
        }
        return processLocally;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }

    public void handleTriggerScaleAck(TriggerScaleAck ack) {
        scaleOutComplete.set(true);
    }

    private synchronized void updateIncomingRatesForSubPrefixes(GeoHashIndexedRecord record) {
        String prefix = record.getGeoHash().substring(0, record.getPrefixLength());
        long[] messageCountsForSubPrefixes;
        double[] messageRatesForSubPrefixes;
        if (messageCountsForSubPrefixesMap.containsKey(prefix)) {
            messageCountsForSubPrefixes = messageCountsForSubPrefixesMap.get(prefix);
            messageRatesForSubPrefixes = messageRatesForSubPrefixesMap.get(prefix);
        } else {
            messageCountsForSubPrefixes = new long[GEO_HASH_LEN_IN_CHARS];
            messageRatesForSubPrefixes = new double[GEO_HASH_LEN_IN_CHARS];
            messageCountsForSubPrefixesMap.put(prefix, messageCountsForSubPrefixes);
            messageRatesForSubPrefixesMap.put(prefix, messageRatesForSubPrefixes);
        }

        int index = getIndexForSubPrefix(record.getGeoHash(), record.getPrefixLength());
        messageCountsForSubPrefixes[index]++;

        long timeNow = System.currentTimeMillis();
        if (lastUpdatedTime == 0) {
            lastUpdatedTime = timeNow;
        } else if ((timeNow - lastUpdatedTime) > INPUT_RATE_UPDATE_INTERVAL) {
            double timeElapsed = (timeNow - lastUpdatedTime) * 1.0;
            StringBuilder sBuilder = new StringBuilder();
            DecimalFormat dFormat = new DecimalFormat();
            for (int i = 0; i < GEO_HASH_LEN_IN_CHARS; i++) {
                messageRatesForSubPrefixes[i] = messageCountsForSubPrefixes[i] / timeElapsed;
                messageCountsForSubPrefixes[i] = 0;
                sBuilder.append(GEO_HASH_CHAR_SET.charAt(i)).append(" - ").append(
                        dFormat.format(messageRatesForSubPrefixes[i])).append(", ");
            }
            System.out.println(sBuilder);
            lastUpdatedTime = timeNow;
        }
    }

    private int getIndexForSubPrefix(String geohash, int prefixLength) {
        return GEO_HASH_CHAR_SET.indexOf(geohash.charAt(prefixLength));
    }

}




