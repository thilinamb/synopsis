package neptune.geospatial.core.protocol.processors.client;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.NeptuneRuntime;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.client.PersistStateRequest;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PersistStateReqProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(PersistStateReqProcessor.class);
    public static final String CHECKPOINT_LOC = "checkpoint-dir";

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        logger.info("Received a state persistence request.");
        PersistStateRequest persistStateRequest = (PersistStateRequest) ctrlMsg;
        long checkpointId = persistStateRequest.getCheckpointId();
        String fileName = getSerializationLocation() + "/" + checkpointId + streamProcessor.getInstanceIdentifier().substring(0, 4);
        DataOutputStream dataOutputStream = null;
        FileOutputStream fos = null;
        boolean success = false;
        try {
            fos = new FileOutputStream(new File(fileName));
            dataOutputStream = new DataOutputStream(fos);
            streamProcessor.serialize(dataOutputStream);
            dataOutputStream.flush();
            success = true;
        } catch (java.io.IOException e) {
            logger.error("Error when serializing the state.", e);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                if (dataOutputStream != null) {
                    dataOutputStream.close();
                }
            } catch (IOException e) {
                logger.error("Error when closing the output streams.", e);
            }
        }
        PersistStateResponse resp = null;
        if (!success) {
            resp = new PersistStateResponse(false);
        } else {
            if (persistStateRequest.isSendBackPrefixTree()) {
                try {
                    byte[] serializedPrefTree = GeoHashPrefixTree.getInstance().serialize();
                    resp = new PersistStateResponse(checkpointId, fileName, streamProcessor.getInstanceIdentifier(),
                            serializedPrefTree);
                } catch (IOException e) {
                    logger.error("Error serializing the trie.", e);
                }
            } else {
                resp = new PersistStateResponse(checkpointId, fileName, streamProcessor.getInstanceIdentifier());
            }
        }
        if (resp != null) {
            try {
                SendUtility.sendControlMessage(persistStateRequest.getClientAddr(), resp);
                logger.info(String.format("[%s] Persisting state is complete. Status: %b",
                        streamProcessor.getInstanceIdentifier(), success));
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending persist state response back to the client.", e);
            }
        }
    }

    private String getSerializationLocation() {
        try {
            return NeptuneRuntime.getInstance().getProperties().getProperty(CHECKPOINT_LOC);
        } catch (GranulesConfigurationException e) {
            logger.error("Error when retrieving checkpoint dir. Using /tmp/.", e);
        }
        return "/tmp/";
    }
}
