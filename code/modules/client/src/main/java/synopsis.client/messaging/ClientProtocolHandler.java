package synopsis.client.messaging;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;

/**
 * @author Thilina Buddhika
 */
public class ClientProtocolHandler extends AbstractProtocolHandler {

    private final Logger logger = Logger.getLogger(ClientProtocolHandler.class);
    private final CountDownLatch startedFlag;

    public ClientProtocolHandler(CountDownLatch startedFlag) {
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
