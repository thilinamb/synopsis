package neptune.geospatial.core.protocol.processors.scalout;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleOutRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutLockResponseProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(ScaleOutLockResponseProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        ScaleOutLockResponse lockResponse = (ScaleOutLockResponse) ctrlMsg;
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(lockResponse.getKey());
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        String targetComputation = lockResponse.getSourceComputation();
        String targetLocCtrlEndpoint = lockResponse.getOriginEndpoint();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] ScaleOutLockResponse from %s. Lock Status: %b", instanceIdentifier,
                    lockResponse.getSourceComputation(), lockResponse.isLockAcquired()));
        }
        if (pendingReq != null) {
            if (lockResponse.isLockAcquired()) {
                for (String prefix : pendingReq.getPrefixes()) {
                    MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
                    monitoredPrefix.setIsPassThroughTraffic(true);
                    monitoredPrefix.setDestComputationId(targetComputation);
                    monitoredPrefix.setDestResourceCtrlEndpoint(targetLocCtrlEndpoint);
                    monitoredPrefix.setOutGoingStream(pendingReq.getStreamId());
                    try {
                        // send a dummy message, just to ensure the new computation is activated.
                        GeoHashIndexedRecord record = new GeoHashIndexedRecord(monitoredPrefix.getLastGeoHashSent(),
                                prefix.length() + 1, -1, System.currentTimeMillis(), new byte[0]);
                        streamProcessor.emit(monitoredPrefix.getOutGoingStream(), record);
                        byte[] state = streamProcessor.split(prefix);
                        StateTransferMsg stateTransferMsg = new StateTransferMsg(prefix, lockResponse.getKey(),
                                state, targetComputation, instanceIdentifier,
                                StateTransferMsg.SCALE_OUT);
                        stateTransferMsg.setLastMessageId(monitoredPrefix.getLastMessageSent());
                        stateTransferMsg.setLastMessagePrefix(monitoredPrefix.getLastGeoHashSent());
                        SendUtility.sendControlMessage(monitoredPrefix.getDestResourceCtrlEndpoint(), stateTransferMsg);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] New Pass-Thru Prefix. Prefix: %s, Outgoing Stream: %s",
                                    instanceIdentifier, prefix, pendingReq.getStreamId()));
                        }
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error transferring state to " + monitoredPrefix.getDestResourceCtrlEndpoint());
                    } catch (StreamingDatasetException e) {
                        logger.error("Error sending a message");
                    }
                }
            } else {
                streamProcessor.releaseMutex();
                try {
                    ManagedResource.getInstance().scalingOperationComplete(instanceIdentifier);
                } catch (NIException ignore) {

                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Failed to get target computation lock from %s. Releasing mutex.", instanceIdentifier,
                            lockResponse.getSourceComputation()));
                }
            }
        } else {
            logger.warn(String.format("[%s] Invalid ScaleOutLockResponse message from %s.", instanceIdentifier,
                    lockResponse.getSourceComputation()));
        }
    }
}
