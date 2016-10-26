package neptune.geospatial.core.protocol.processors;

import com.hazelcast.core.IMap;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.FullQualifiedComputationAddr;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleInRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.msg.scalein.ScaleInComplete;
import neptune.geospatial.core.protocol.msg.scaleout.StateTransferCompleteAck;
import neptune.geospatial.hazelcast.type.SketchLocation;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class StateTransferMsgProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(StateTransferMsgProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        StateTransferMsg stateTransferMsg = (StateTransferMsg) ctrlMsg;
        boolean scaleType = stateTransferMsg.isScaleType();
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();

        if (scaleType) { // scale-in
            PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(stateTransferMsg.getKeyPrefix());
            boolean completedStateTransfers = pendingReq.trackStateTransfer(stateTransferMsg.getPrefix());
            streamProcessor.merge(stateTransferMsg.getPrefix(), stateTransferMsg.getSerializedData());
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s]Received a StateTransferMsg. Prefix: %s, Key Prefix: %s, Remaining: %d",
                        instanceIdentifier, stateTransferMsg.getPrefix(), stateTransferMsg.getKeyPrefix(),
                        pendingReq.getChildLeafPrefixes().size()));
            }
            if (completedStateTransfers) {
                completeScaleIn(stateTransferMsg.getKeyPrefix(), instanceIdentifier, pendingReq);
            }
        } else {    // scale-out
            try {
                streamProcessor.merge(stateTransferMsg.getPrefix(), stateTransferMsg.getSerializedData());
            } catch (Throwable e) {
                logger.error(e);
                dumpTransferredStateToDisk(stateTransferMsg.getSerializedData(), stateTransferMsg.getPrefix());
            }
            String childPrefix = streamProcessor.getPrefix(stateTransferMsg.getLastMessagePrefix(),
                    stateTransferMsg.getPrefix().length());
            // handling the case where no messages are sent after scaling out.
            synchronized (scalingContext) {
                if (scalingContext.getMonitoredPrefix(childPrefix) == null) {
                    MonitoredPrefix monitoredPrefix = new MonitoredPrefix(childPrefix, stateTransferMsg.getStreamType());
                    monitoredPrefix.setLastMessageSent(stateTransferMsg.getLastMessageId());
                    monitoredPrefix.setLastGeoHashSent(stateTransferMsg.getLastMessagePrefix());
                    scalingContext.addMonitoredPrefix(childPrefix, monitoredPrefix);
                    try {
                        IMap<String, SketchLocation> prefMap = streamProcessor.getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                        prefMap.put(childPrefix, new SketchLocation(instanceIdentifier, RivuletUtil.getCtrlEndpoint(),
                                SketchLocation.MODE_REGISTER_NEW_PREFIX));
                    } catch (GranulesConfigurationException e) {
                        logger.error("Error publishing to Hazelcast.", e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Messages haven't arrived after scaling out. " +
                                        "Setting last message scene to: %d", instanceIdentifier,
                                stateTransferMsg.getLastMessageId()));
                    }
                }
            }
            if (!stateTransferMsg.isAcked()) {
                StateTransferCompleteAck ack = new StateTransferCompleteAck(
                        stateTransferMsg.getKeyPrefix(), stateTransferMsg.getPrefix(),
                        stateTransferMsg.getOriginComputation());
                try {
                    SendUtility.sendControlMessage(stateTransferMsg.getOriginEndpoint(), ack);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error acknowledging the parent on the state transfer.", e);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a state transfer message for the prefix: %s during the " +
                        "scaling out.", instanceIdentifier, stateTransferMsg.getPrefix()));
            }
        }
    }

    private void completeScaleIn(String prefix, String instanceIdentifier, PendingScaleInRequest pendingReq) {
        // initiate the scale-in complete request.
        for (Map.Entry<String, FullQualifiedComputationAddr> participant : pendingReq.getSentOutRequests().entrySet()) {
            ScaleInComplete scaleInComplete = new ScaleInComplete(prefix, participant.getValue().getComputationId(),
                    instanceIdentifier);
            try {
                SendUtility.sendControlMessage(participant.getValue().getCtrlEndpointAddr(), scaleInComplete);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received all StateTransferMsgs. " +
                                    "Initiating ProtocolEnd message flow. Prefix: %s",
                            instanceIdentifier, prefix));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending out ScaleInComplete to " + participant.getValue().getCtrlEndpointAddr(), e);
            }
        }
        pendingReq.setReceivedCount(0);
    }

    private void dumpTransferredStateToDisk(byte[] state, String prefix){
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            String fName = "/tmp/state-" + prefix + ".dump";
            fos = new FileOutputStream(fName);
            dos = new DataOutputStream(fos);
            dos.writeInt(state.length);
            dos.write(state);
            dos.flush();
            fos.flush();
            logger.info("Transferred state saved as " + fName);
        } catch (IOException e) {
            logger.error("Error dumping the transfered state for analysis.", e);
        } finally {
            try {
                if(fos != null){
                    fos.close();
                }
                if(dos != null){
                    dos.close();
                }
            } catch (IOException e) {
                logger.error("Error closing file stream.", e);
            }
        }
    }
}
