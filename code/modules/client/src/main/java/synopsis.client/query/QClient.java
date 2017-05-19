package synopsis.client.query;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import io.sigpipe.sing.adapters.QueryTest;
import io.sigpipe.sing.serialization.SerializationException;
import neptune.geospatial.core.protocol.msg.client.ClientQueryRequest;
import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;
import org.apache.log4j.Logger;
import synopsis.client.ClientException;
import synopsis.client.SynopsisEndpoint;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Simulates a client who continuously submits queries.
 *
 * @author Thilina Buddhika
 */
public class QClient implements Runnable {

    private Logger logger = Logger.getLogger(QClient.class);
    public static final int WARMUP_THRESHOLD = 100;
    private BlockingQueue<ControlMessage> queue = new LinkedBlockingDeque<>();
    private final int queryCount;
    private QueryWrapper[] queries;
    private QueryCreator.QueryType[] queryTypes;
    private double[] cumulativePercentages;
    private final Random random;
    private final List<SynopsisEndpoint> endpoints;
    private final QueryManager queryManager;
    private final String clientUrl;
    private final QClientStatRecorder statRecorder;
    private CountDownLatch latch;
    private boolean manualMode = false;
    // gather some queries in SQL format to use with Spark
    private BufferedWriter sqlQueryWriter;

    public QClient(int queryCount, QueryWrapper[] queries, QueryCreator.QueryType[] queryTypes, double[] percentages,
                   String clientUrl, QueryManager queryManager,
                   List<SynopsisEndpoint> endpoints, CountDownLatch latch)
            throws ClientException {
        if (queries.length != percentages.length) {
            throw new ClientException("Query count doesn't match the percentage distribution.");
        }
        this.queryCount = queryCount + WARMUP_THRESHOLD;
        this.queries = queries;
        this.queryTypes = queryTypes;
        this.cumulativePercentages = prepareCumulPercentages(percentages);
        this.random = new Random();
        this.queryManager = queryManager;
        this.endpoints = endpoints;
        this.clientUrl = clientUrl;
        this.statRecorder = new QClientStatRecorder();
        this.latch = latch;
        this.manualMode = true;
    }

    public QClient(int queryCount, List<SynopsisEndpoint> endpoints, String clientUrl, QueryManager queryManager,
                   CountDownLatch latch) {
        this.queryCount = queryCount;
        this.statRecorder = new QClientStatRecorder();
        this.endpoints = endpoints;
        this.queryManager = queryManager;
        this.clientUrl = clientUrl;
        this.latch = latch;
        this.random = new Random();
        try {
            this.sqlQueryWriter = new BufferedWriter(new FileWriter("/tmp/sql-queries.txt"));
        } catch (IOException e) {
            logger.error("Error initializing SQL writer.", e);
        }
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
        String statisticsOutputFile = "/tmp/" + clientUrl.split(":")[0] + "-" + clientUrl.split(":")[1] + "-" +
                Thread.currentThread().getId() + ".qstat";
        int completedQueryCount = 0;
        while (completedQueryCount <= this.queryCount) {
            QueryWrapper nextQ;
            QueryCreator.QueryType queryType;
            if (manualMode) {
                int index = nextQueryIndex();
                if (index != -1) {
                    nextQ = queries[index];
                    queryType = queryTypes[index];
                } else {
                    logger.error("[" + id + "] Next query is null!, QClient is terminating!");
                    break;
                }
            } else {
                nextQ = QueryCreator.create();
                queryType = QueryCreator.QueryType.Relational;
            }
            try {
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
                queryManager.removeClient(queryId);
                if (logger.isDebugEnabled()) {
                    logger.debug("[" + id + "] Query " + queryId + " is complete!. Completed: " +
                            completedQueryCount + "[" + queryCount + "]");
                }
                /*statRecorder.record(queryType, currentQueryResponse.getElapsedTimeInMS(),
                        currentQueryResponse.getQueryResponseSizeInKB());*/
                if (completedQueryCount > WARMUP_THRESHOLD && currentQueryResponse.getQueryRespSize() > 0) {
                    statRecorder.recordIndividualRecord("metadata", currentQueryResponse.getElapsedTimeInMS(),
                            currentQueryResponse.getQueryResponseSizeInKB(), statisticsOutputFile);
                    // Get the SQL equivalent form of the query.
                    if (sqlQueryWriter != null) {
                        try {
                            sqlQueryWriter.write(QueryTest.getSQLFormat(nextQ) + '\n');
                        } catch (SerializationException e) {
                            logger.error("Error generating SQL equalent form of the query.", e);
                        }
                    }
                }

            } catch (ClientException | IOException e) {
                logger.error("[" + id + "] Error instantiating the query request.", e);
            } catch (CommunicationsException e) {
                logger.error("[" + id + "] Error sending out the query request.", e);
            } catch (InterruptedException e) {
                logger.error("[" + id + "] Error retrieving messages from the internal queue.", e);
            }
        }

        try {
            statRecorder.finalizeStats();
            if (sqlQueryWriter != null) {
                sqlQueryWriter.flush();
                sqlQueryWriter.close();
            }
        } catch (IOException e) {
            logger.error("Error flushing outputs.", e);
        }
        logger.info("[" + id + "] Completed running all queries.");
        latch.countDown();
    }

    private int nextQueryIndex() {
        double rand = random.nextDouble();
        for (int i = 0; i < cumulativePercentages.length; i++) {
            if (cumulativePercentages[i] >= rand) {
                return i;
            }
        }
        return -1;
    }

    private String nextEndpoint() {
        SynopsisEndpoint ep = endpoints.get(random.nextInt(endpoints.size()));
        return ep.getHostname() + ":" + ep.getControlPort();
    }

    void handleQueryResponse(ControlMessage ctrlMsg) {
        queue.add(ctrlMsg);
    }

    public synchronized QClientStatRecorder getStatRecorder() {
        return statRecorder;
    }
}
