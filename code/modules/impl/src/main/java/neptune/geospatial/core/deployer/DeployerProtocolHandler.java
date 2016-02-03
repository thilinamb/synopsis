package neptune.geospatial.core.deployer;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.TriggerScale;
import org.apache.log4j.Logger;

/**
 * Handles the protocol messages from the deployer's end.
 *
 * @author Thilina Buddhika
 */
public class DeployerProtocolHandler extends AbstractProtocolHandler {

    private final GeoSpacialDeployer geoSpacialDeployer;
    private Logger logger = Logger.getLogger(DeployerProtocolHandler.class);

    public DeployerProtocolHandler(GeoSpacialDeployer geoSpacialDeployer) {
        this.geoSpacialDeployer = geoSpacialDeployer;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        switch (type) {
            case ProtocolTypes.TRIGGER_SCALING:
                TriggerScale triggerScale = (TriggerScale) ctrlMsg;
                if(logger.isDebugEnabled()){
                    logger.debug("Received a trigger scale message from " + triggerScale.getCurrentComputation());
                }
        }
    }

    @Override
    public void notifyStartup() {
        geoSpacialDeployer.ackCtrlMsgHandlerStarted();
    }
}
