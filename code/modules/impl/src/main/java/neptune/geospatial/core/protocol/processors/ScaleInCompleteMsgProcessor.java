package neptune.geospatial.core.protocol.processors;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.FullQualifiedComputationAddr;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleInRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleInComplete;
import neptune.geospatial.core.protocol.msg.ScaleInCompleteAck;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class ScaleInCompleteMsgProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleInCompleteMsgProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleInComplete scaleInCompleteMsg = (ScaleInComplete) ctrlMsg;
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(scaleInCompleteMsg.getPrefix());

        if (pendingReq.getSentOutRequests().entrySet().size() > 0) {
            pendingReq.setReceivedCount(0);
            for (Map.Entry<String, FullQualifiedComputationAddr> participant : pendingReq.getSentOutRequests().entrySet()) {
                try {
                    ScaleInComplete scaleInComplete = new ScaleInComplete(scaleInCompleteMsg.getPrefix(),
                            participant.getValue().getComputationId(), instanceIdentifier);
                    SendUtility.sendControlMessage(participant.getValue().getCtrlEndpointAddr(), scaleInComplete);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Forwarding the ScaleInComplete message to child nodes. " +
                                        "Key Prefix: %s, Child Prefix: %s", instanceIdentifier,
                                scaleInCompleteMsg.getPrefix(), participant.getKey()));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending out ScaleInComplete to " + participant.getValue().getCtrlEndpointAddr(), e);
                }
            }
        } else {
            try {
                ScaleInCompleteAck ack = new ScaleInCompleteAck(scaleInCompleteMsg.getPrefix(),
                        scaleInCompleteMsg.getParentComputation());
                SendUtility.sendControlMessage(scaleInCompleteMsg.getOriginEndpoint(), ack);
                PendingScaleInRequest pendingScaleInRequest = scalingContext.getPendingScalingInRequest(
                        scaleInCompleteMsg.getPrefix());
                // stop monitoring the prefixes that are scaled in
                for (String locallyProcessedPrefix : pendingScaleInRequest.getLocallyProcessedPrefixes()){
                    scalingContext.removeMonitoredPrefix(locallyProcessedPrefix);
                }
                scalingContext.removePendingScaleInRequest(scaleInCompleteMsg.getPrefix());
                streamProcessor.releaseMutex();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] No Child Prefixes. Unlocking the mutex at node.",
                            instanceIdentifier));
                }
            } catch (CommunicationsException | IOException e) {
                e.printStackTrace();
            }
        }
    }
}
