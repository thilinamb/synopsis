package synopsis.client.query;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class OutstandingQueryRegistry {

    private static final OutstandingQueryRegistry instance = new OutstandingQueryRegistry();
    private final Logger logger = Logger.getLogger(OutstandingQueryRegistry.class);

    private Map<Long, QueryResponse> outstandingQueries = new ConcurrentHashMap<>();
    private Map<Long, QueryCallback> callbacks = new ConcurrentHashMap<>();

    private OutstandingQueryRegistry() {
        // singleton
    }

    public static OutstandingQueryRegistry getInstance() {
        return instance;
    }

    public void addOutstandingQuery(long queryId, QueryResponse queryResponse, QueryCallback queryCallback) {
        outstandingQueries.put(queryId, queryResponse);
        callbacks.put(queryId, queryCallback);
        logger.info("Outstanding query was added.  Query Id: " + queryId + ", Size: " + outstandingQueries.size());
    }

    public void removeOutstandingQuery(long queryId) {
        outstandingQueries.remove(queryId);
        callbacks.remove(queryId);
        logger.info("Outstanding query was removed. Query Id: " + queryId + ", Size: " + outstandingQueries.size());
    }

    public QueryResponse getOutstandingQueryResponse(long queryId) {
        return outstandingQueries.get(queryId);
    }

    public QueryCallback getQueryCallback(long queryId){
        return callbacks.get(queryId);
    }

}
