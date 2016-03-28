package neptune.geospatial.core.protocol.processors.scalein;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.FullQualifiedComputationAddr;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleInRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleInLockRequest;
import neptune.geospatial.core.protocol.msg.ScaleInLockResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class ScaleInLockReqProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleInLockReqProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleInLockRequest lockReq = (ScaleInLockRequest) ctrlMsg;
        String prefixForLock = lockReq.getPrefix();
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        boolean lockAvailable = streamProcessor.tryAcquireMutex();

        if (!lockAvailable) {
            ScaleInLockResponse lockResp = new ScaleInLockResponse(false, lockReq.getPrefix(),
                    lockReq.getSourceComputation(), null);
            try {
                SendUtility.sendControlMessage(lockReq.getOriginEndpoint(), lockResp);
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending back the lock response to " + lockReq.getOriginEndpoint());
            }
        } else {
            // need to find out the available sub prefixes
            Map<String, FullQualifiedComputationAddr> requestsSentOut = new HashMap<>();
            List<String> locallyProcessedPrefixes = new ArrayList<>();
            List<MonitoredPrefix> childPrefixes = scalingContext.getChildPrefixesForScalingIn(prefixForLock);
            for (MonitoredPrefix pref : childPrefixes) {
                if (pref.getIsPassThroughTraffic()) {
                    ScaleInLockRequest childLockReq = new ScaleInLockRequest(prefixForLock, instanceIdentifier,
                            pref.getDestComputationId());
                    try {
                        SendUtility.sendControlMessage(pref.getDestResourceCtrlEndpoint(), childLockReq);
                        //lockedSubTrees.get().add(monitoredPrefix);
                        requestsSentOut.put(pref.getPrefix(), new FullQualifiedComputationAddr(
                                pref.getDestResourceCtrlEndpoint(), pref.getDestComputationId()));
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Propagating lock requests to child elements. " +
                                            "Parent prefix: %s, Child Prefix: %s, Child Resource: %s, " +
                                            "Child Computation Id: %s", instanceIdentifier,
                                    prefixForLock, pref.getPrefix(), pref.getDestResourceCtrlEndpoint(),
                                    pref.getDestComputationId()));
                        }
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending a lock request to child for prefix: " + pref.getPrefix());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Found a locally processed prefix for ScaleInLock. " +
                                        "Parent Prefix: %s, Child Prefix: %s", instanceIdentifier,
                                prefixForLock, pref.getPrefix()));
                    }
                    locallyProcessedPrefixes.add(pref.getPrefix());
                }

            }
            PendingScaleInRequest pendingScaleInRequest = new PendingScaleInRequest(prefixForLock,
                    requestsSentOut.size(), lockReq.getOriginEndpoint(), lockReq.getSourceComputation());
            pendingScaleInRequest.setLocallyProcessedPrefix(locallyProcessedPrefixes);
            pendingScaleInRequest.setSentOutRequests(requestsSentOut);
            scalingContext.addPendingScalingInRequest(prefixForLock, pendingScaleInRequest);

            // breaking condition of the recursive calls.
            if (requestsSentOut.size() == 0) {
                ScaleInLockResponse lockResp = new ScaleInLockResponse(true, prefixForLock,
                        lockReq.getSourceComputation(), locallyProcessedPrefixes);
                try {
                    SendUtility.sendControlMessage(lockReq.getOriginEndpoint(), lockResp);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] No Child Sub-prefixes found for prefix: %s. " +
                                        "Acknowledging parent: %s, %s", instanceIdentifier, prefixForLock,
                                lockReq.getOriginEndpoint(), lockReq.getSourceComputation()));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending back the lock response to " + lockReq.getOriginEndpoint());
                }
            }
        }
    }
}
