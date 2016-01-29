package neptune.geospatial.core.deployer;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.AbstractProtocolHandler;

/**
 * Handles the protocol messages from the deployer's end.
 *
 * @author Thilina Buddhika
 */
public class DeployerProtocolHandler extends AbstractProtocolHandler {

    private final Deployer deployer;

    public DeployerProtocolHandler(Deployer deployer) {
        this.deployer = deployer;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {

    }

    @Override
    public void notifyStartup() {
        deployer.ackCtrlMsgHandlerStarted();
    }
}
