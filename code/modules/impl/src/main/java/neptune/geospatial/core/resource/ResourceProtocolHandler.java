package neptune.geospatial.core.resource;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.ScaleInLockRequest;
import neptune.geospatial.core.protocol.msg.ScaleOutResponse;
import org.apache.log4j.Logger;

/**
 * Implements the control message handling at the Resource's
 * end.
 *
 * @author Thilina Buddhika
 */
public class ResourceProtocolHandler extends AbstractProtocolHandler {

    private Logger logger = Logger.getLogger(ResourceProtocolHandler.class);

    private final ManagedResource managedResource;

    public ResourceProtocolHandler(ManagedResource managedResource) {
        this.managedResource = managedResource;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        switch (type) {
            case ProtocolTypes.SCALE_OUT_RESP:
                ScaleOutResponse scaleOutResponse = (ScaleOutResponse) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a trigger scale ack message for " + scaleOutResponse.getTargetComputation());
                }
                managedResource.handleTriggerScaleAck(scaleOutResponse);
                break;
            case ProtocolTypes.SCALE_IN_LOCK_REQ:
                ScaleInLockRequest lockReq = (ScaleInLockRequest) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInLockRequest for " + lockReq.getTargetComputation());
                }
                managedResource.handleScaleInLockReq(lockReq);
                break;
            default:
                logger.warn("Unsupported message type: " + type);
        }

    }

    @Override
    public void notifyStartup() {
        managedResource.acknowledgeCtrlMsgListenerStartup();
    }
}
