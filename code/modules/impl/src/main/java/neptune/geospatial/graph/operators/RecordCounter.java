package neptune.geospatial.graph.operators;

import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.core.computations.GeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.msg.TriggerScale;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
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
            TriggerScale triggerMessage = new TriggerScale(getInstanceIdentifier());
            try {
                ManagedResource.getInstance().sendToDeployer(triggerMessage);
                // this is just to control the scaling to one instance for testing.
                scaledOut = true;
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Sent a trigger scale message to deployer.",
                            getInstanceIdentifier()));
                }
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
