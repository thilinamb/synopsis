package neptune.geospatial.core.protocol.processors;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleOutRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleOutResponse;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Handles {@code ScaleOutResponse} from the deployer
 *
 * @author Thilina Buddhika
 */
public class ScaleOutResponseProcessor implements ProtocolProcessor{

    private Logger logger = Logger.getLogger(ScaleOutResponseProcessor.class);

    @Override
    public void process(ControlMessage ctrlMessage, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleOutResponse scaleOutResp = (ScaleOutResponse)ctrlMessage;
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(scaleOutResp.getInResponseTo());
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();

        if (pendingReq != null) {
            for (String prefix : pendingReq.getPrefixes()) {
                MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
                monitoredPrefix.setIsPassThroughTraffic(true);
                monitoredPrefix.setDestComputationId(scaleOutResp.getNewComputationId());
                monitoredPrefix.setDestResourceCtrlEndpoint(scaleOutResp.getNewLocationURL());
                monitoredPrefix.setOutGoingStream(pendingReq.getStreamId());
                try {
                    // send a dummy message, just to ensure the new computation is activated.
                    GeoHashIndexedRecord record = new GeoHashIndexedRecord(monitoredPrefix.getLastGeoHashSent(),
                            prefix.length() + 1, -1, System.currentTimeMillis(), new byte[0]);
                    streamProcessor.emit(monitoredPrefix.getOutGoingStream(), record);
                    byte[] state = streamProcessor.split(prefix);
                    StateTransferMsg stateTransferMsg = new StateTransferMsg(prefix, scaleOutResp.getInResponseTo(),
                            state, scaleOutResp.getNewComputationId(), instanceIdentifier,
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
            logger.warn("Invalid trigger scaleOutResp for the prefix. Request Id: " + scaleOutResp.getInResponseTo());
        }
    }
}
