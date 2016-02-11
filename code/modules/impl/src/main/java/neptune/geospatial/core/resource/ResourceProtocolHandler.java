package neptune.geospatial.core.resource;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.TriggerScaleAck;
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
                TriggerScaleAck triggerScaleAck = (TriggerScaleAck) ctrlMsg;
                if (logger.isDebugEnabled()) {
                    logger.debug("Received a trigger scale ack message for " + triggerScaleAck.getTargetComputation());
                }
                managedResource.handleTriggerScaleAck(triggerScaleAck);
        }
    }

    @Override
    public void notifyStartup() {
        managedResource.acknowledgeCtrlMsgListenerStartup();
    }
}
