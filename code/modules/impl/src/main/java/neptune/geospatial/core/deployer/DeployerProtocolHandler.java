package neptune.geospatial.core.deployer;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.scaleout.DeploymentAck;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutRequest;
import org.apache.log4j.Logger;

/**
 * Handles the protocol messages from the deployer's end.
 *
 * @author Thilina Buddhika
 */
public class DeployerProtocolHandler extends AbstractProtocolHandler {

    private final GeoSpatialDeployer geoSpatialDeployer;
    private Logger logger = Logger.getLogger(DeployerProtocolHandler.class);

    public DeployerProtocolHandler(GeoSpatialDeployer geoSpatialDeployer) {
        this.geoSpatialDeployer = geoSpatialDeployer;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        try {
            switch (type) {
                case ProtocolTypes.SCALE_OUT_REQ:
                    ScaleOutRequest scaleOutRequest = (ScaleOutRequest) ctrlMsg;
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received a trigger scale message from " + scaleOutRequest.getCurrentComputation());
                    }
                    geoSpatialDeployer.handleScaleUpRequest(scaleOutRequest);
                    break;
                case ProtocolTypes.DEPLOYMENT_ACK:
                    DeploymentAck deploymentAck = (DeploymentAck)ctrlMsg;
                    if(logger.isDebugEnabled()){
                        logger.debug("Received a deployment ack from " + deploymentAck.getInstanceIdentifier());
                    }
                    geoSpatialDeployer.handleDeploymentAck(deploymentAck);
                    break;
            }
        } catch (GeoSpatialDeployerException e) {
            logger.error("Error handling message " + type + ". Error: " + e.getMessage(), e);
        }
    }

    @Override
    public void notifyStartup() {
        geoSpatialDeployer.ackCtrlMsgHandlerStarted();
    }
}
