package neptune.geospatial.core.resource;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.*;
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
            case ProtocolTypes.SCALE_IN_LOCK_RESP:
                ScaleInLockResponse lockResponse = (ScaleInLockResponse) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInLockResponse for " + lockResponse.getComputation());
                }
                managedResource.handleScaleInLockResp(lockResponse);
                break;
            case ProtocolTypes.SCALE_IN_ACTIVATION_REQ:
                ScaleInActivateReq activateReq = (ScaleInActivateReq) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInActivateReq for " + activateReq.getTargetComputation());
                }
                managedResource.handleScaleInActivateReq(activateReq);
                break;
            case ProtocolTypes.STATE_TRANSFER_MSG:
                StateTransferMsg stateTransferMsg = (StateTransferMsg) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a StateTransferMessage for " + stateTransferMsg.getTargetComputation());
                }
                managedResource.handleStateTransferMsg(stateTransferMsg);
                break;
            case ProtocolTypes.SCALE_IN_COMPLETE:
                ScaleInComplete completeMsg = (ScaleInComplete) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInComplete for " + completeMsg.getTargetComputation());
                }
                managedResource.handleScaleInCompleteMsg(completeMsg);
                break;
            case ProtocolTypes.SCALE_IN_COMPLETE_ACK:
                ScaleInCompleteAck ack = (ScaleInCompleteAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInComplete for " + ack.getTargetComputation());
                }
                managedResource.handleScaleInCompleteAckMsg(ack);
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
