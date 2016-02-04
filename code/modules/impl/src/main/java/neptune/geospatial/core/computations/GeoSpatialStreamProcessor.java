package neptune.geospatial.core.computations;


import ds.funnel.topic.Topic;
import ds.granules.dataset.StreamEvent;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.protocol.msg.TriggerScale;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

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

    private int counter = 0;

    private boolean scaledOut = false;
    private String outGoingStream;
    private GeoHashPartitioner partitioner;
    private Map<String, PendingScaleOutRequests> pendingScaleOutRequestsMap = new HashMap<>();

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        // preprocess each message
        preprocess(geoHashIndexedRecord);
        // perform the business logic
        onEvent(geoHashIndexedRecord);
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
    protected void preprocess(GeoHashIndexedRecord record) {
        // this is a temporary trigger to test the scaling out.
        if (!scaledOut & counter >= 1000000) {
            try {
                // FIXME: BEGIN - Need to do it only once
                this.outGoingStream = getStreamIdentifier(OUTGOING_STREAM);
                this.partitioner = new GeoHashPartitioner();
                String streamType = record.getClass().getName();
                declareStream(this.outGoingStream, streamType);
                // FIXME: END
                // initialize the meta-data
                Topic[] topics = deployStream(this.outGoingStream, 1, this.partitioner);

                TriggerScale triggerMessage = new TriggerScale(getInstanceIdentifier(), this.outGoingStream,
                        topics[0].toString(), streamType);
                ManagedResource.getInstance().sendToDeployer(triggerMessage);

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Sent a trigger scale message to deployer.",
                            getInstanceIdentifier()));
                }
                // this is just to control the scaling to one instance for testing.
                scaledOut = true;
                // FIXME: Increase the prefix length of the record if needed. May be inside the onEvent method.
            } catch (StreamingDatasetException | StreamingGraphConfigurationException e) {
                logger.error("Error creating new stream from the current computation.", e);
            } catch (NIException e) {
                logger.error("Error sending a trigger scale message to the deployer. ", e);
            }
        }

    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }

}




