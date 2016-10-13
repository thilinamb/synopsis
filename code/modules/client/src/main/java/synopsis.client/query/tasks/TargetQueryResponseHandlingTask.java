package synopsis.client.query.tasks;

import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import org.apache.log4j.Logger;
import synopsis.client.query.OutstandingQueryRegistry;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryResponse;

/**
 * Handles responses coming back from individual Synopsis
 * nodes with responses
 *
 * @author Thilina Buddhika
 */
public class TargetQueryResponseHandlingTask implements Runnable {

    private Logger logger = Logger.getLogger(TargetQueryResponseHandlingTask.class);
    private final TargetQueryResponse targetQueryResponse;

    public TargetQueryResponseHandlingTask(TargetQueryResponse targetQueryResponse) {
        this.targetQueryResponse = targetQueryResponse;
    }

    @Override
    public void run() {
        long queryId = targetQueryResponse.getQueryId();
        OutstandingQueryRegistry registry = OutstandingQueryRegistry.getInstance();
        QueryResponse queryResponse = registry.getOutstandingQueryResponse(queryId);
        if (queryResponse != null) {
            boolean fireCallback = queryResponse.addQueryResponse(
                    targetQueryResponse.getResponse(), targetQueryResponse.getQueryEvalTime());
            if (fireCallback) {
                logger.info("Callback is ready to be invoked for query: " + queryId);
                QueryCallback callback = registry.getQueryCallback(queryId);
                if (callback != null){
                    callback.processQueryResponse(queryResponse);
                } else {
                    logger.error("No callback registered for query: " + queryId);
                }
                registry.removeOutstandingQuery(queryId);
            }
        }
    }
}
