package synopsis.client.query.tasks;

import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.protocol.msg.client.ClientQueryRequest;
import org.apache.log4j.Logger;
import synopsis.client.query.OutstandingQueryRegistry;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryResponse;

import java.io.IOException;

/**
 * Submit a query to a random Synopsis node
 *
 * @author Thilina Buddhika
 */
public class QuerySubmitTask implements Runnable {

    private Logger logger = Logger.getLogger(QuerySubmitTask.class);
    private long queryId;
    private String targetNode;
    private ClientQueryRequest queryRequest;
    private QueryCallback callback;

    public QuerySubmitTask(long queryId, String targetNode, ClientQueryRequest queryRequest, QueryCallback callback) {
        this.queryId = queryId;
        this.targetNode = targetNode;
        this.queryRequest = queryRequest;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            QueryResponse queryResponse = new QueryResponse(queryId, queryRequest.getQuery());
            OutstandingQueryRegistry.getInstance().addOutstandingQuery(queryId, queryResponse, this.callback);
            SendUtility.sendControlMessage(this.targetNode, this.queryRequest);
        } catch (CommunicationsException | IOException e) {
            logger.error("Error sending query request to " + this.targetNode, e);
            OutstandingQueryRegistry.getInstance().removeOutstandingQuery(queryId);
        }
    }
}
