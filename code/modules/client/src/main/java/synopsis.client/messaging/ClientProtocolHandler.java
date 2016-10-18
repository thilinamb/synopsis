package synopsis.client.messaging;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.ProtocolTypes;
import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import neptune.geospatial.core.protocol.msg.client.PersistStateAck;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import org.apache.log4j.Logger;
import synopsis.client.persistence.PersistenceManager;
import synopsis.client.query.QueryManager;

import java.util.concurrent.CountDownLatch;

/**
 * @author Thilina Buddhika
 */
public class ClientProtocolHandler extends AbstractProtocolHandler {

    private final Logger logger = Logger.getLogger(ClientProtocolHandler.class);
    private final CountDownLatch startedFlag;
    private final QueryManager queryManager;
    private final PersistenceManager persistenceManager;

    public ClientProtocolHandler(CountDownLatch startedFlag, QueryManager queryManager) {
        this.startedFlag = startedFlag;
        this.queryManager = queryManager;
        this.persistenceManager = PersistenceManager.getInstance();
    }


    @Override
    public void handle(ControlMessage ctrlMsg) {
        int type = ctrlMsg.getMessageType();
        if (logger.isDebugEnabled()) {
            logger.debug("Dispatching ctrl message. Type: " + type);
        }
        switch (type) {
            case ProtocolTypes.CLIENT_QUERY_RESP:
                queryManager.handleClientQueryResponse((ClientQueryResponse) ctrlMsg);
                break;
            case ProtocolTypes.TARGET_QUERY_RESP:
                queryManager.handleTargetQueryResponse((TargetQueryResponse) ctrlMsg);
                break;
            case ProtocolTypes.PERSIST_STATE_ACK:
                persistenceManager.handlePersistenceAck((PersistStateAck) ctrlMsg);
                break;
            case ProtocolTypes.PERSIST_STATE_RESP:
                persistenceManager.handlePersistStateResponse((PersistStateResponse) ctrlMsg);
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
