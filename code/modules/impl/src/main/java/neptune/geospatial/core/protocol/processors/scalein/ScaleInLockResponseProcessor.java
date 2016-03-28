package neptune.geospatial.core.protocol.processors.scalein;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.FullQualifiedComputationAddr;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleInRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleInActivateReq;
import neptune.geospatial.core.protocol.msg.ScaleInComplete;
import neptune.geospatial.core.protocol.msg.ScaleInLockResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class ScaleInLockResponseProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleInLockResponseProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleInLockResponse lockResponse = (ScaleInLockResponse) ctrlMsg;
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        String prefix = lockResponse.getPrefix();
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(prefix);

        if (pendingReq == null) {
            logger.warn("Invalid ScaleInLockResponse for prefix: " + prefix);
        } else {
            int receivedCount = pendingReq.incrementAndGetReceivedCount();
            boolean lockAcquired = pendingReq.updateAndGetLockStatus(lockResponse.isSuccess());
            if (lockResponse.getLeafPrefixes() != null) {
                pendingReq.addChildPrefixes(lockResponse.getLeafPrefixes());
            }
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleInLockResponse. Prefix: %s, " +
                                "SentCount: %d, ReceivedCount: %d, Lock Acquired: %b",
                        instanceIdentifier, prefix, pendingReq.getSentCount(), receivedCount, lockAcquired));
            }
            if (pendingReq.getSentCount() == receivedCount) {
                if (pendingReq.isInitiatedLocally()) {
                    if (lockAcquired) {
                        for (String lockedPrefix : pendingReq.getSentOutRequests().keySet()) {
                            // disable pass-through
                            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
                            monitoredPrefix.setIsPassThroughTraffic(false);
                            FullQualifiedComputationAddr reqInfo = pendingReq.getSentOutRequests().get(lockedPrefix);
                            try {
                                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(prefix,
                                        reqInfo.getComputationId(), monitoredPrefix.getLastMessageSent(),
                                        monitoredPrefix.getLastGeoHashSent(), monitoredPrefix.getPrefix().length(),
                                        RivuletUtil.getCtrlEndpoint(), instanceIdentifier);
                                SendUtility.sendControlMessage(reqInfo.getCtrlEndpointAddr(), scaleInActivateReq);

                                if (logger.isDebugEnabled()) {
                                    logger.debug(String.format("[%s] Sent a ScaleInActivateReq for parent prefix: %s, " +
                                                    "child prefix: %s, To: %s -> %s, Last Message Sent: %d, Geohash: %s",
                                            instanceIdentifier, prefix, monitoredPrefix.getPrefix(),
                                            reqInfo.getCtrlEndpointAddr(), reqInfo.getComputationId(),
                                            monitoredPrefix.getLastMessageSent(),
                                            monitoredPrefix.getLastGeoHashSent()));
                                }
                            } catch (CommunicationsException | IOException | GranulesConfigurationException e) {
                                logger.error("Error sending ScaleInActivationRequest to " +
                                        reqInfo.getCtrlEndpointAddr(), e);
                            }
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Failed to acquire lock for prefix %s. Resetting locks.",
                                    instanceIdentifier, prefix));
                        }
                        // haven't been able to acquire all the locks. Reset the locks.
                        completeScaleIn(prefix, instanceIdentifier, pendingReq);
                    }
                } else { // send the parent the status
                    List<String> leafPrefixes = new ArrayList<>();
                    leafPrefixes.addAll(pendingReq.getLocallyProcessedPrefixes());
                    leafPrefixes.addAll(pendingReq.getChildLeafPrefixes());
                    ScaleInLockResponse lockRespToParent = new ScaleInLockResponse(lockAcquired, pendingReq.getPrefix(),
                            pendingReq.getOriginComputation(), leafPrefixes);
                    try {
                        SendUtility.sendControlMessage(pendingReq.getOriginCtrlEndpoint(), lockRespToParent);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Acknowledging parent for lock status. " +
                                            "Prefix: %s, Lock Acquired: %b", instanceIdentifier, prefix,
                                    lockAcquired));
                        }
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending ScaleInLockResponse to " + lockRespToParent);
                    }
                }
            }
        }
    }

    private void completeScaleIn(String prefix, String instanceId, PendingScaleInRequest pendingReq) {
        // initiate the scale-in complete request.
        for (Map.Entry<String, FullQualifiedComputationAddr> participant : pendingReq.getSentOutRequests().entrySet()) {
            ScaleInComplete scaleInComplete = new ScaleInComplete(prefix, participant.getValue().getComputationId(),
                    instanceId);
            try {
                SendUtility.sendControlMessage(participant.getValue().getCtrlEndpointAddr(), scaleInComplete);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received all StateTransferMsgs. " +
                                    "Initiating ProtocolEnd message flow. Prefix: %s", instanceId, prefix));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending out ScaleInComplete to " + participant.getValue().getCtrlEndpointAddr(), e);
            }
        }
        pendingReq.setReceivedCount(0);
    }
}
