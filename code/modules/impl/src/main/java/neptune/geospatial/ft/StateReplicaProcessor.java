package neptune.geospatial.ft;

import ds.funnel.topic.Topic;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.dataset.DatasetException;
import ds.granules.dataset.StreamEvent;
import ds.granules.exception.CommunicationsException;
import ds.granules.operation.ProcessingException;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.StreamUtil;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.ft.protocol.CheckpointAck;
import neptune.geospatial.graph.Constants;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * Used for replicating state of {@link neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor}
 * implementations.
 *
 * @author Thilina Buddhika
 */
public class StateReplicaProcessor extends StreamProcessor {

    private Logger logger = Logger.getLogger(StateReplicaProcessor.class);
    public static String CHECK_PT_STORAGE_LOC = File.pathSeparator + "tmp" + File.pathSeparator + "states";

    @Override
    public void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        StateReplicationMessage stateReplicationMsg = (StateReplicationMessage) streamEvent;
        byte[] bytes = stateReplicationMsg.getSerializedState();
        storeReplicatedState(bytes, stateReplicationMsg.getCheckpointId());
        if (logger.isInfoEnabled()) {
            logger.info(String.format("Received a state replication message. Primary: %s, Checkpoint: %s",
                    stateReplicationMsg.getPrimaryComp(), stateReplicationMsg.getCheckpointId()));
        }
        // acknowledge the primary
        CheckpointAck ack = new CheckpointAck(CheckpointAck.ACK_FROM_STATE_REPLICATOR, CheckpointAck.STATUS_SUCCESS,
                stateReplicationMsg.getCheckpointId(), stateReplicationMsg.getPrimaryComp());
        try {
            SendUtility.sendControlMessage(stateReplicationMsg.getPrimaryCompLocation(), ack);
        } catch (CommunicationsException | IOException e) {
            logger.error("Error acknowledging primary upon state persistence.", e);
        }
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // no out-going streams
    }

    public void registerIncomingTopic(Topic topic, String jobId) throws StreamingDatasetException {
        try {
            if (!isInitialized()) {
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

    private void storeReplicatedState(byte[] bytes, long checkPointId) {
        File storageDir = new File(CHECK_PT_STORAGE_LOC);
        if (!storageDir.exists()) {
            boolean success = storageDir.mkdir();
            if (!success) {
                logger.error("Error when creating a dir for storing checkpoints.");
                return;
            }
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(CHECK_PT_STORAGE_LOC + File.pathSeparator + checkPointId);
                fos.write(bytes);
                fos.flush();
                logger.info("Successfully written the received state checkpoint to the disk.");
            } catch (IOException e) {
                logger.error("Error writing the replicated state!", e);
            } finally {
                try {
                    if (fos != null) {
                        fos.close();
                    }
                } catch (IOException e) {
                   logger.error("Error closing file streams.", e);
                }
            }
        }
    }
}
