package synopsis.client.messaging;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import org.apache.log4j.Logger;
import synopsis.client.query.QueryManager;

import java.util.concurrent.CountDownLatch;

/**
 * @author Thilina Buddhika
 */
public class ClientProtocolHandler extends AbstractProtocolHandler {

    private final Logger logger = Logger.getLogger(ClientProtocolHandler.class);
    private final CountDownLatch startedFlag;
    private final QueryManager queryManager;

    public ClientProtocolHandler(CountDownLatch startedFlag, QueryManager queryManager) {
        this.startedFlag = startedFlag;
        this.queryManager = queryManager;
    }

    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        switch (type){
            case ProtocolTypes.CLIENT_QUERY_RESP:
                queryManager.handleClientQueryResponse((ClientQueryResponse) ctrlMsg);
                break;
            case ProtocolTypes.TARGET_QUERY_RESP:
                queryManager.handleTargetQueryResponse((TargetQueryResponse) ctrlMsg);
                break;
            default:
                logger.warn("Unsupported control message. Type: " + type);
        }
    }

    @Override
    public void notifyStartup() {
        startedFlag.countDown();
    }
}
