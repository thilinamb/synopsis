package neptune.geospatial.ft;

import ds.funnel.topic.Topic;
import ds.granules.dataset.DatasetException;
import ds.granules.dataset.StreamEvent;
import ds.granules.operation.ProcessingException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.StreamUtil;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.graph.Constants;
import org.apache.log4j.Logger;

import java.util.Properties;

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

    public void registerIncomingTopic(Topic topic, String jobId) throws StreamingDatasetException {
        try {
            if(!isInitialized()){
                Properties initProps = getInitProperties(StreamUtil.getInstanceIdentifier(
                        Constants.Operators.STATE_REPLICA_PROCESSOR_NAME), jobId);
                this.initialize(initProps);
            }
            this.getDefaultStreamDataset().addInputStream(topic, this.getInstanceIdentifier());
            // add the incoming stream type at the destination.
            this.incomingStreamTypes.put(topic.toString(), StateReplicationMessage.class.getName());
            this.identifierMap.put(Integer.parseInt(topic.toString()), Constants.Streams.STATE_REPLICA_STREAM);
        } catch (DatasetException | ProcessingException e) {
            logger.error(e.getMessage(), e);
            throw new StreamingDatasetException(e.getMessage(), e);
        }
    }

    private Properties getInitProperties(String streamBaseName, String jobId) {
        Properties initProps = new Properties();
        initProps.setProperty(ds.granules.util.Constants.STREAM_PROP_JOB_ID, jobId);
        initProps.setProperty(ds.granules.util.Constants.STREAM_PROP_STREAM_BASE_ID, streamBaseName);
        return initProps;
    }
}
