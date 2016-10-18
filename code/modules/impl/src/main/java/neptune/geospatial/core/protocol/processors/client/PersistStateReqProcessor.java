package neptune.geospatial.core.protocol.processors.client;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.client.PersistStateRequest;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PersistStateReqProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(PersistStateReqProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        PersistStateRequest persistStateRequest = (PersistStateRequest) ctrlMsg;
        long checkpointId = persistStateRequest.getCheckpointId();
        String fileName = getSerializationLocation() + "/" + checkpointId;
        DataOutputStream dataOutputStream = null;
        FileOutputStream fos = null;
        boolean success = false;
        try {
            fos = new FileOutputStream(fileName);
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
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending persist state response back to the client.", e);
            }
        }
    }

    private String getSerializationLocation() {
        return "/s/" + RivuletUtil.getHostInetAddress().getHostName() + "/a/nobackup/thilinab/dumps";
    }
}
