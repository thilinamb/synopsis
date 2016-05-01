package neptune.geospatial.core.protocol;

import ds.funnel.topic.TopicDataEvent;
import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.msg.*;
import neptune.geospatial.core.protocol.msg.scalein.*;
import neptune.geospatial.core.protocol.msg.scaleout.*;
import neptune.geospatial.ft.protocol.CheckpointAck;
import neptune.geospatial.ft.protocol.StateReplicationLevelIncreaseMsg;
import neptune.geospatial.stat.InstanceRegistration;
import neptune.geospatial.stat.PeriodicInstanceMetrics;
import neptune.geospatial.stat.ScaleActivity;
import neptune.geospatial.stat.StatConstants;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Parses each <code>TopicDataEvent</code> and returns an
 * instance of the appropriate control message.
 * This is a singleton class.
 *
 * @author Thilina Buddhika
 */
public class ProtocolFactory {

    private static ProtocolFactory instance = new ProtocolFactory();

    private ProtocolFactory(){
        // singleton, hence a private constructor
    }

    public static ProtocolFactory getInstance(){
        return instance;
    }

    public ControlMessage parse(TopicDataEvent topicDataEvent) throws ProtocolException {
        int messageType = getMessageType(topicDataEvent.getDataBytes());
        try {
            ControlMessage message;
            switch (messageType) {
                case ProtocolTypes.SCALE_OUT_REQ:
                    message = new ScaleOutRequest();
                    break;
                case ProtocolTypes.SCALE_OUT_RESP:
                    message = new ScaleOutResponse();
                    break;
                case ProtocolTypes.SCALE_OUT_LOCK_REQ:
                    message = new ScaleOutLockRequest();
                    break;
                case ProtocolTypes.SCALE_OUT_LOCK_RESP:
                    message = new ScaleOutLockResponse();
                    break;
                case ProtocolTypes.SCALE_OUT_COMPLETE:
                    message = new ScaleOutCompleteMsg();
                    break;
                case ProtocolTypes.SCALE_OUT_COMPLETE_ACK:
                    message = new ScaleOutCompleteAck();
                    break;
                case ProtocolTypes.DEPLOYMENT_ACK:
                    message = new DeploymentAck();
                    break;
                case ProtocolTypes.STATE_TRANSFER_COMPLETE_ACK:
                    message = new StateTransferCompleteAck();
                    break;
                case ProtocolTypes.SCALE_IN_LOCK_REQ:
                    message = new ScaleInLockRequest();
                    break;
                case ProtocolTypes.SCALE_IN_LOCK_RESP:
                    message = new ScaleInLockResponse();
                    break;
                case ProtocolTypes.SCALE_IN_ACTIVATION_REQ:
                    message = new ScaleInActivateReq();
                    break;
                case ProtocolTypes.STATE_TRANSFER_MSG:
                    message = new StateTransferMsg();
                    break;
                case ProtocolTypes.SCALE_IN_COMPLETE:
                    message = new ScaleInComplete();
                    break;
                case ProtocolTypes.SCALE_IN_COMPLETE_ACK:
                    message = new ScaleInCompleteAck();
                    break;
                case ProtocolTypes.STATE_REPL_LEVEL_INCREASE:
                    message = new StateReplicationLevelIncreaseMsg();
                    break;
                case ProtocolTypes.CHECKPOINT_ACK:
                    message = new CheckpointAck();
                    break;
                case StatConstants.MessageTypes.REGISTER:
                    message = new InstanceRegistration();
                    break;
                case StatConstants.MessageTypes.PERIODIC_UPDATE:
                    message = new PeriodicInstanceMetrics();
                    break;
                case StatConstants.MessageTypes.STAT_ACTIVITY:
                    message = new ScaleActivity();
                    break;
                case ProtocolTypes.PREFIX_ONLY_SCALE_OUT_COMPLETE:
                    message = new PrefixOnlyScaleOutCompleteAck();
                    break;
                default:
                    String errorMsg = "Unsupported message type: " + messageType;
                    throw new ProtocolException(errorMsg);
            }
            message.unmarshall(topicDataEvent.getDataBytes());
            return message;
        } catch (IOException e) {
            throw new ProtocolException(e.getMessage(), e);
        }
    }

    private int getMessageType(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        try {
            return dis.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bais.close();
                dis.close();
            } catch (IOException ignore) {
            }
        }
        return -1;
    }
}
