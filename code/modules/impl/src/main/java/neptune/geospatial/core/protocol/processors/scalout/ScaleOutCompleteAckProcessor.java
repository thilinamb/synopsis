package neptune.geospatial.core.protocol.processors.scalout;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.neptune.interfere.core.NIException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleOutRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutCompleteAck;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.core.resource.ManagedResource;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutCompleteAckProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(ScaleOutCompleteAckProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        ScaleOutCompleteAck ack = (ScaleOutCompleteAck) ctrlMsg;
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(ack.getKey());
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Scaling out complete for now.", instanceIdentifier));
            }
            scalingContext.completeScalingOut(ack.getKey());
            streamProcessor.onSuccessfulScaleOut(pendingReq.getPrefixes());
            streamProcessor.releaseMutex();
            ManagedResource.getInstance().scalingOperationComplete(instanceIdentifier);
        } catch (NIException ignore) {

        }
    }
}
