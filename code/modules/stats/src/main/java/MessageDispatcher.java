import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.stat.InstanceRegistration;
import neptune.geospatial.stat.PeriodicInstanceMetrics;
import neptune.geospatial.stat.StatMessageTypes;
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
    private final StatManager statManager;

    MessageDispatcher(StatsServer statsServer) {
        this.statsServer = statsServer;
        this.statManager = StatManager.getInstance();
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        switch (type) {
            case StatMessageTypes.REGISTER:
                statManager.register((InstanceRegistration) ctrlMsg);
                break;
            case StatMessageTypes.PERIODIC_UPDATE:
                statManager.updateMetrics((PeriodicInstanceMetrics)ctrlMsg);
                break;
            default:
        }
    }

    @Override
    public void notifyStartup() {
        statsServer.notifyDispatcherStartup();
    }
}
