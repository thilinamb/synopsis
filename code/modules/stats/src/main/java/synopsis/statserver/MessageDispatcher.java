package synopsis.statserver;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.stat.InstanceRegistration;
import neptune.geospatial.stat.PeriodicInstanceMetrics;
import neptune.geospatial.stat.ScaleActivity;
import neptune.geospatial.stat.StatConstants;
import org.apache.log4j.Logger;

/**
 * Dispatch messages received at the stat server endpoint to the stat manager for
 * processing.
 *
 * @author Thilina Buddhika
 */
class MessageDispatcher extends AbstractProtocolHandler {

    private final Logger logger = Logger.getLogger(MessageDispatcher.class);
    private final StatsServer statsServer;
    private final StatRegistry statRegistry;

    MessageDispatcher(StatsServer statsServer) {
        this.statsServer = statsServer;
        this.statRegistry = StatRegistry.getInstance();
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        switch (type) {
            case StatConstants.MessageTypes.REGISTER:
                statRegistry.register((InstanceRegistration) ctrlMsg);
                break;
            case StatConstants.MessageTypes.PERIODIC_UPDATE:
                statRegistry.updateMetrics((PeriodicInstanceMetrics)ctrlMsg);
                break;
            case StatConstants.MessageTypes.STAT_ACTIVITY:
                statRegistry.processScalingActivity((ScaleActivity)ctrlMsg);
                break;
            default:
                logger.warn("Invalid message type: " + type);
        }
    }

    @Override
    public void notifyStartup() {
        statsServer.notifyDispatcherStartup();
    }
}
