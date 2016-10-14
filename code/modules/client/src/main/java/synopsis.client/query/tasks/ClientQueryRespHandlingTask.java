package synopsis.client.query.tasks;

import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import org.apache.log4j.Logger;
import synopsis.client.query.OutstandingQueryRegistry;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryResponse;

/**
 * Handles the response coming from the Synopsis node that is first contacted.
 *
 * @author Thilina Buddhika
 */
public class ClientQueryRespHandlingTask implements Runnable {

    private final ClientQueryResponse clientQueryResponse;
    private final Logger logger = Logger.getLogger(ClientQueryRespHandlingTask.class);

    public ClientQueryRespHandlingTask(ClientQueryResponse clientQueryResponse) {
        this.clientQueryResponse = clientQueryResponse;
    }

    @Override
    public void run() {
        long queryId = clientQueryResponse.getQueryId();
        QueryResponse queryResponse = OutstandingQueryRegistry.getInstance().getOutstandingQueryResponse(queryId);
        if (queryResponse != null) {
            boolean fireCallback = queryResponse.setExpectedQueryResponseCount(clientQueryResponse.getTargetCompCount());
            if (fireCallback) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Ready to fire the callback. All responses are received.");
                }
                QueryCallback callback = OutstandingQueryRegistry.getInstance().getQueryCallback(queryId);
                if (callback != null) {
                    callback.processQueryResponse(queryResponse);
                } else {
                    logger.error("No callback is registered for query: " + queryId);
                }
                OutstandingQueryRegistry.getInstance().removeOutstandingQuery(queryId);
            }
        } else {
            logger.error("Invalid query id: " + queryId + ", no entry in outstanding query registry.");
        }
    }
}
