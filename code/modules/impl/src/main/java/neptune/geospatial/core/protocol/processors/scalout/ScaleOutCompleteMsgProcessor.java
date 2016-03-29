package neptune.geospatial.core.protocol.processors.scalout;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutCompleteAck;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutCompleteMsg;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutCompleteMsgProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleOutCompleteMsgProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        ScaleOutCompleteMsg scaleOutCompleteMsg = (ScaleOutCompleteMsg) ctrlMsg;
        streamProcessor.releaseMutex();
        ScaleOutCompleteAck ack = new ScaleOutCompleteAck(scaleOutCompleteMsg.getKey(),
                streamProcessor.getInstanceIdentifier(), scaleOutCompleteMsg.getSourceComputation());
        try {
            SendUtility.sendControlMessage(scaleOutCompleteMsg.getOriginEndpoint(), ack);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Released mutex after scale out. Sending ack back to the origin.",
                        streamProcessor.getInstanceIdentifier()));
            }
        } catch (CommunicationsException | IOException e) {
            logger.error("Error sending ScaleOutCompleteAck back to " + scaleOutCompleteMsg.getOriginEndpoint(), e);
        }
    }
}
