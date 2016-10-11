package synopsis.client.messaging;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;

/**
 * @author Thilina Buddhika
 */
public class ClientMessageDispatcher extends AbstractProtocolHandler {

    private final Logger logger = Logger.getLogger(ClientMessageDispatcher.class);
    private final CountDownLatch startedFlag;

    public ClientMessageDispatcher(CountDownLatch startedFlag) {
        this.startedFlag = startedFlag;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {

    }

    @Override
    public void notifyStartup() {
        startedFlag.countDown();
    }
}
