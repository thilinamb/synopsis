package neptune.geospatial.core.resource;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.EnableShortCircuiting;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.msg.client.ClientQueryRequest;
import neptune.geospatial.core.protocol.msg.client.PersistStateRequest;
import neptune.geospatial.core.protocol.msg.client.PersistStateAck;
import neptune.geospatial.core.protocol.msg.client.TargetedQueryRequest;
import neptune.geospatial.core.protocol.msg.scalein.*;
import neptune.geospatial.core.protocol.msg.scaleout.*;
import neptune.geospatial.ft.protocol.CheckpointAck;
import neptune.geospatial.ft.protocol.StateReplicationLevelIncreaseMsg;
import org.apache.log4j.Logger;

import java.io.IOException;

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
            case ProtocolTypes.SCALE_OUT_COMPLETE:
                ScaleOutCompleteMsg scaleOutCompleteMsg = (ScaleOutCompleteMsg) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleOutCompleteMsg for " + scaleOutCompleteMsg.getTargetComputation());
                }
                managedResource.dispatchControlMessage(scaleOutCompleteMsg.getTargetComputation(), scaleOutCompleteMsg);
                break;
            case ProtocolTypes.SCALE_OUT_COMPLETE_ACK:
                ScaleOutCompleteAck scaleOutCompleteAck = (ScaleOutCompleteAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleOutCompleteAck for " + scaleOutCompleteAck.getTargetComputation());
                }
                managedResource.dispatchControlMessage(scaleOutCompleteAck.getTargetComputation(), scaleOutCompleteAck);
                break;
            case ProtocolTypes.STATE_TRANSFER_COMPLETE_ACK:
                StateTransferCompleteAck stateTransferCompleteAck = (StateTransferCompleteAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a ScaleOutCompleteAck for " + stateTransferCompleteAck.getTargetComputation());
                }
                managedResource.dispatchControlMessage(stateTransferCompleteAck.getTargetComputation(), stateTransferCompleteAck);
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
            case ProtocolTypes.STATE_REPL_LEVEL_INCREASE:
                StateReplicationLevelIncreaseMsg replIncreaseMsg = (StateReplicationLevelIncreaseMsg) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a state replication level increase message for " +
                            replIncreaseMsg.getTargetComputation());
                }
                managedResource.dispatchControlMessage(replIncreaseMsg.getTargetComputation(), replIncreaseMsg);
                break;
            case ProtocolTypes.CHECKPOINT_ACK:
                CheckpointAck checkpointAck = (CheckpointAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a State persistence ack for " + checkpointAck.getTargetComputation());
                }
                managedResource.dispatchControlMessage(checkpointAck.getTargetComputation(), checkpointAck);
                break;
            case ProtocolTypes.PREFIX_ONLY_SCALE_OUT_COMPLETE:
                PrefixOnlyScaleOutCompleteAck prefixOnlyScaleOutCompleteAck = (PrefixOnlyScaleOutCompleteAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a PrefixOnlyScaleOutCompleteAck from " +
                            prefixOnlyScaleOutCompleteAck.getOriginEndpoint());
                }
                managedResource.dispatchPrefixOnlyScaleOutAck(prefixOnlyScaleOutCompleteAck);
                break;
            case ProtocolTypes.ENABLE_SHORT_CIRCUITING:
                EnableShortCircuiting shortCircuitingMsg = (EnableShortCircuiting) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a EnableShortCircuiting from the deployer.");
                }
                managedResource.dispatchEnableShortCircuitingMessage(shortCircuitingMsg);
                break;
            case ProtocolTypes.CLIENT_QUERY_REQ:
                ClientQueryRequest clientQueryRequest = (ClientQueryRequest) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a query request.");
                }
                managedResource.handleQueryRequest(clientQueryRequest);
                break;
            case ProtocolTypes.TARGET_QUERY_REQ:
                TargetedQueryRequest queryRequest = (TargetedQueryRequest) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a target query request.");
                }
                for (String compId : queryRequest.getCompId()) {
                    managedResource.dispatchControlMessage(compId, queryRequest);
                }
                break;
            case ProtocolTypes.PERSIST_STATE_REQ:
                PersistStateRequest persistStateRequest = (PersistStateRequest) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a persist state request.");
                }
                int processorCount = managedResource.dispatchToAll(persistStateRequest);
                PersistStateAck persistenceAck = new  PersistStateAck(processorCount, persistStateRequest.getCheckpointId());
                try {
                    SendUtility.sendControlMessage(persistStateRequest.getClientAddr(), persistenceAck);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending Persistence Ack back to the client.", e);
                }
                break;
            case ProtocolTypes.UPDATE_PREFIX_TREE:
                logger.info("Received a update prefix tree request.");
                managedResource.dispatchToAll(ctrlMsg);
                break;
            case ProtocolTypes.TERMINATE_NODE:
                logger.info("Received a terminate node request. Terminating!");
                System.exit(0);
            default:
                logger.warn("Unsupported message type: " + type);
        }

    }

    @Override
    public void notifyStartup() {
        managedResource.acknowledgeCtrlMsgListenerStartup();
    }
}
