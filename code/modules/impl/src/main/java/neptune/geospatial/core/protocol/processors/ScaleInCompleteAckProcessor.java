package neptune.geospatial.core.protocol.processors;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.neptune.interfere.core.NIException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleInRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleInCompleteAck;
import neptune.geospatial.core.resource.ManagedResource;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleInCompleteAckProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleInCompleteAckProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleInCompleteAck ack = (ScaleInCompleteAck) ctrlMsg;
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(ack.getPrefix());

        if (pendingReq != null) {
            int receivedCount = pendingReq.incrementAndGetReceivedCount();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleInCompleteAck from a child. " +
                                "Sent Ack Count: %d, Received Ack Count: %d", instanceIdentifier,
                        pendingReq.getSentCount(), receivedCount));
            }
            if (receivedCount == pendingReq.getSentCount()) {
                if (!pendingReq.isInitiatedLocally()) {
                    ScaleInCompleteAck ackToParent = new ScaleInCompleteAck(ack.getPrefix(),
                            pendingReq.getOriginComputation());
                    // stop monitoring the prefixes that are scaled in
                    for (String locallyProcessedPrefix : pendingReq.getLocallyProcessedPrefixes()){
                        scalingContext.removeMonitoredPrefix(locallyProcessedPrefix);
                    }
                    try {
                        SendUtility.sendControlMessage(pendingReq.getOriginCtrlEndpoint(), ackToParent);
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending out a ScaleInCompleteAck to " + pendingReq.getOriginCtrlEndpoint());
                    }
                } else { // initiated locally.
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Completed Scaling in for prefix : %s",
                                instanceIdentifier, ack.getPrefix()));
                    }
                    // update hazelcast
                    /*IMap<String, SketchLocation> prefMap = getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                        prefMap.put(ack.getPrefix(), new SketchLocation(getInstanceIdentifier(), getCtrlEndpoint(),
                                SketchLocation.MODE_SCALE_IN)); */
                    streamProcessor.onSuccessfulScaleIn(pendingReq.getChildLeafPrefixes());
                }
                scalingContext.removePendingScaleInRequest(ack.getPrefix());
                streamProcessor.releaseMutex();
                try {
                    ManagedResource.getInstance().scalingOperationComplete(instanceIdentifier);
                } catch (NIException ignore) {

                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Received all child ScaleInCompleteAcks. Acknowledging parent.",
                            instanceIdentifier));
                }
            }
        }
    }
}
