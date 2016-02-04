package neptune.geospatial.graph.operators;

import ds.funnel.topic.Topic;
import ds.granules.Granules;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.computations.GeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.msg.TriggerScale;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import org.apache.log4j.Logger;

/**
 * This is a temporary operator used to count
 * the number of records in order to make sure
 * initial setup works.
 *
 * @author Thilina Buddhika
 */
public class RecordCounter extends GeoSpatialStreamProcessor {

    private Logger logger = Logger.getLogger(RecordCounter.class);
    private int counter = 0;
    private boolean scaledOut = false;

    @Override
    protected void onEvent(GeoHashIndexedRecord record) {
        if (++counter % 100000 == 0) {
            logger.info(String.format("[" + getInstanceIdentifier() + "] Record received. Counter: %d Hash: %s " +
                    "Timestamp: %d", counter, record.getGeoHash(), record.getTsIngested()));
        }
    }

    @Override
    protected void preprocess(GeoHashIndexedRecord record) {
        // this is a temporary trigger to test the scaling out.
        if (!scaledOut & counter >= 1000000) {
            try {
                // initialize the meta-data
                String streamId = Granules.getUUIDRetriever().getRandomBasedUUIDAsString();
                String streamType =  record.getClass().getName();
                declareStream(streamId, streamType);
                Topic[] topics = deployStream(streamId, 1, GeoHashPartitioner.class.getName());

                TriggerScale triggerMessage = new TriggerScale(getInstanceIdentifier(), streamId, topics[0].toString(),
                        streamType);
                ManagedResource.getInstance().sendToDeployer(triggerMessage);

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Sent a trigger scale message to deployer.",
                            getInstanceIdentifier()));
                }

                // this is just to control the scaling to one instance for testing.
                scaledOut = true;
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
