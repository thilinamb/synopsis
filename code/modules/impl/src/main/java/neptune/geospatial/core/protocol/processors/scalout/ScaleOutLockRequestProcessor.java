package neptune.geospatial.core.protocol.processors.scalout;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutLockRequestProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(ScaleOutLockRequestProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        ScaleOutLockRequest lockRequest = (ScaleOutLockRequest) ctrlMsg;
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        String sourceComputation = lockRequest.getSourceComputation();
        boolean lockStatus = streamProcessor.tryAcquireMutex();
        ScaleOutLockResponse lockResponse = new ScaleOutLockResponse(lockRequest.getKey(), instanceIdentifier, sourceComputation, lockStatus);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Processed ScaleOutLockRequest from %s. Lock Granted: %b", instanceIdentifier,
                    sourceComputation, lockStatus));
        }
        try {
            SendUtility.sendControlMessage(lockRequest.getOriginEndpoint(), lockResponse);
        } catch (CommunicationsException | IOException e) {
            streamProcessor.releaseMutex();
            logger.error(String.format("[%s] Error sending back ScaleOutLockResponse to %s",
                    instanceIdentifier, lockRequest.getOriginEndpoint()), e);
        }
    }
}
