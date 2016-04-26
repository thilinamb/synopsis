import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;

/**
 * @author Thilina Buddhika
 */
public class MessageDispatcher extends AbstractProtocolHandler {

    private final StatsServer statsServer;

    public MessageDispatcher(StatsServer statsServer) {
        this.statsServer = statsServer;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {

    }

    @Override
    public void notifyStartup() {
        statsServer.notifyDispatcherStartup();
    }
}
