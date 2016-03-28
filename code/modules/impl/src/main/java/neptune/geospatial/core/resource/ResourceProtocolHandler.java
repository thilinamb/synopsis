package neptune.geospatial.core.resource;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.*;
import neptune.geospatial.core.protocol.msg.scalein.*;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutCompleteAck;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockResponse;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutResponse;
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
                managedResource.dispatchControlMessage(scaleOutResponse.getTargetComputation(), scaleOutResponse);
                break;
            case ProtocolTypes.SCALE_OUT_LOCK_REQ:
                ScaleOutLockRequest scaleOutLockRequest = (ScaleOutLockRequest) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleOutLockRequest for " + scaleOutLockRequest.getTargetComputation());
                }
                managedResource.dispatchControlMessage(scaleOutLockRequest.getTargetComputation(), scaleOutLockRequest);
                break;
            case ProtocolTypes.SCALE_OUT_LOCK_RESP:
                ScaleOutLockResponse scaleOutLockResponse = (ScaleOutLockResponse) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleOutLockResponse for " + scaleOutLockResponse.getTargetComputation());
                }
                managedResource.dispatchControlMessage(scaleOutLockResponse.getTargetComputation(), scaleOutLockResponse);
                break;
            case ProtocolTypes.SCALE_OUT_COMPLETE_ACK:
                ScaleOutCompleteAck scaleOutCompleteAck = (ScaleOutCompleteAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleOutCompleteAck for " + scaleOutCompleteAck.getTargetComputation());
                }
                managedResource.dispatchControlMessage(scaleOutCompleteAck.getTargetComputation(), scaleOutCompleteAck);
                break;
            case ProtocolTypes.SCALE_IN_LOCK_REQ:
                ScaleInLockRequest lockReq = (ScaleInLockRequest) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInLockRequest for " + lockReq.getTargetComputation());
                }
                managedResource.dispatchControlMessage(lockReq.getTargetComputation(), lockReq);
                break;
            case ProtocolTypes.SCALE_IN_LOCK_RESP:
                ScaleInLockResponse lockResponse = (ScaleInLockResponse) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInLockResponse for " + lockResponse.getComputation());
                }
                managedResource.dispatchControlMessage(lockResponse.getComputation(), lockResponse);
                break;
            case ProtocolTypes.SCALE_IN_ACTIVATION_REQ:
                ScaleInActivateReq activateReq = (ScaleInActivateReq) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInActivateReq for " + activateReq.getTargetComputation());
                }
                managedResource.dispatchControlMessage(activateReq.getTargetComputation(), activateReq);
                break;
            case ProtocolTypes.STATE_TRANSFER_MSG:
                StateTransferMsg stateTransferMsg = (StateTransferMsg) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a StateTransferMessage for " + stateTransferMsg.getTargetComputation());
                }
                managedResource.dispatchControlMessage(stateTransferMsg.getTargetComputation(), stateTransferMsg);
                break;
            case ProtocolTypes.SCALE_IN_COMPLETE:
                ScaleInComplete completeMsg = (ScaleInComplete) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInComplete for " + completeMsg.getTargetComputation());
                }
                managedResource.dispatchControlMessage(completeMsg.getTargetComputation(), completeMsg);
                break;
            case ProtocolTypes.SCALE_IN_COMPLETE_ACK:
                ScaleInCompleteAck ack = (ScaleInCompleteAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleInComplete for " + ack.getTargetComputation());
                }
                managedResource.dispatchControlMessage(ack.getTargetComputation(), ack);
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
