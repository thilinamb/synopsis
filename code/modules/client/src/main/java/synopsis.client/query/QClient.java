package synopsis.client.query;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.protocol.msg.client.ClientQueryRequest;
import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import neptune.geospatial.graph.operators.QueryWrapper;
import org.apache.log4j.Logger;
import synopsis.client.ClientException;
import synopsis.client.SynopsisEndpoint;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Simulates a client who continuously submits queries.
 *
 * @author Thilina Buddhika
 */
public class QClient implements Runnable {

    private Logger logger = Logger.getLogger(QClient.class);
    private BlockingQueue<ControlMessage> queue = new LinkedBlockingDeque<>();
    private final int queryCount;
    private final QueryWrapper[] queries;
    private final double[] cumulativePercentages;
    private final Random random;
    private final List<SynopsisEndpoint> endpoints;
    private final QueryManager queryManager;
    private final String clientUrl;

    public QClient(int queryCount, QueryWrapper[] queries, double[] percentages, String clientUrl, QueryManager queryManager,
                   List<SynopsisEndpoint> endpoints)
            throws ClientException {
        if (queries.length != percentages.length) {
            throw new ClientException("Query count doesn't match the percentage distribution.");
        }
        this.queryCount = queryCount;
        this.queries = queries;
        this.cumulativePercentages = prepareCumulPercentages(percentages);
        this.random = new Random();
        this.queryManager = queryManager;
        this.endpoints = endpoints;
        this.clientUrl = clientUrl;
    }

    private double[] prepareCumulPercentages(double[] percentages) throws ClientException {
        double[] cumulativePercentages = new double[percentages.length];
        double sum = 0.0;
        for (int i = 0; i < percentages.length; i++) {
            sum += percentages[i];
            cumulativePercentages[i] = sum;
        }
        return cumulativePercentages;
    }

    @Override
    public void run() {
        String id = this.clientUrl + ":" + Thread.currentThread().getName();
        int completedQueryCount = 0;
        while (completedQueryCount <= this.queryCount) {
            try {
                QueryWrapper nextQ = nextQuery();
                if (nextQ != null) {
                    long queryId = queryManager.getNextQueryId();
                    ClientQueryRequest queryRequest = new ClientQueryRequest(queryId, clientUrl, nextQ.payload, nextQ.geohashes);
                    QueryResponse currentQueryResponse = new QueryResponse(queryId, nextQ.payload);
                    queryManager.registerQClient(this, queryId);
                    SendUtility.sendControlMessage(nextEndpoint(), queryRequest);
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + id + "] Query " + queryId + " is submitted.");
                    }
                    boolean queryComplete = false;
                    while (!queryComplete) {
                        ControlMessage controlMessage = queue.take();
                        if (controlMessage instanceof ClientQueryResponse) {
                            ClientQueryResponse queryResponse = (ClientQueryResponse) controlMessage;
                            queryComplete = currentQueryResponse.setExpectedQueryResponseCount(queryResponse.getTargetCompCount());
                        } else if (controlMessage instanceof TargetQueryResponse) {
                            TargetQueryResponse targetQueryResponse = (TargetQueryResponse) controlMessage;
                            queryComplete = currentQueryResponse.addQueryResponse(targetQueryResponse.getResponse(),
                                    targetQueryResponse.getQueryEvalTime());
                        }
                    }
                    completedQueryCount++;
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + id + "] Query " + queryId + " is complete!. Completed: " +
                                completedQueryCount + "[" + queryCount + "]");
                    }
                    queryManager.removeClient(queryId);
                } else {
                    logger.error("[" + id + "] Next query is null!, QClient is terminating!");
                    break;
                }
            } catch (ClientException | IOException e) {
                logger.error("[" + id + "] Error instantiating the query request.", e);
            } catch (CommunicationsException e) {
                logger.error("[" + id + "] Error sending out the query request.", e);
            } catch (InterruptedException e) {
                logger.error("[" + id + "] Error retrieving messages from the internal queue.", e);
            }
        }
        logger.info("[" + id + "] Completed running all queries.");
    }

    private QueryWrapper nextQuery() {
        double rand = random.nextDouble();
        for (int i = 0; i < cumulativePercentages.length; i++) {
            if (cumulativePercentages[i] >= rand) {
                return queries[i];
            }
        }
        return null;
    }

    private String nextEndpoint() {
        return endpoints.get(random.nextInt(endpoints.size())).toString();
    }

    void handleQueryResponse(ControlMessage ctrlMsg) {
        queue.add(ctrlMsg);
    }
}
