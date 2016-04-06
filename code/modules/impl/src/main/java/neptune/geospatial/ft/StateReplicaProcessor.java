package neptune.geospatial.ft;

import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import org.apache.log4j.Logger;

/**
 * Used for replicating state of {@link neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor}
 * implementations.
 *
 * @author Thilina Buddhika
 */
public class StateReplicaProcessor extends StreamProcessor {

    private Logger logger = Logger.getLogger(StateReplicaProcessor.class);

    @Override
    public void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        logger.debug("Received a state replication message");
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // no out-going streams
    }
}
