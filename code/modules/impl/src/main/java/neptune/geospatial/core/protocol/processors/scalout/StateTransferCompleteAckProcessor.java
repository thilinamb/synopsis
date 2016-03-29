package neptune.geospatial.core.protocol.processors.scalout;

import com.hazelcast.core.IMap;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleOutRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutCompleteMsg;
import neptune.geospatial.core.protocol.msg.scaleout.StateTransferCompleteAck;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.hazelcast.type.SketchLocation;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class StateTransferCompleteAckProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(StateTransferCompleteAckProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        StateTransferCompleteAck ack = (StateTransferCompleteAck) ctrlMsg;
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(ack.getKey());
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();

        if (pendingReq != null) {
            int ackCount = pendingReq.incrementAndGetAckCount();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleOutCompleteAck. Sent Count: %d, Received Count: %d",
                        instanceIdentifier, pendingReq.getPrefixes().size(), ackCount));
            }
            // update prefix tree
            IMap<String, SketchLocation> prefMap = streamProcessor.getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(ack.getPrefix());
            prefMap.put(ack.getPrefix(), new SketchLocation(monitoredPrefix.getDestComputationId(),
                    monitoredPrefix.getDestResourceCtrlEndpoint(), SketchLocation.MODE_SCALE_OUT));
            // finalize the scale out operation
            if (ackCount == pendingReq.getPrefixes().size()) {
                ScaleOutCompleteMsg completeMsg = new ScaleOutCompleteMsg(ack.getKey(), instanceIdentifier,
                        monitoredPrefix.getDestComputationId());
                try {
                    SendUtility.sendControlMessage(monitoredPrefix.getDestResourceCtrlEndpoint(), completeMsg);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Received all state transfer acks. " +
                                "Sent ScaleOutComplete Message.", instanceIdentifier));
                    }
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending ScaleOutCompleteMsg to " + monitoredPrefix.getDestResourceCtrlEndpoint());
                }
            }
        } else {
            logger.warn("Invalid ScaleOutCompleteAck for " + ack.getKey());
        }
    }
}
