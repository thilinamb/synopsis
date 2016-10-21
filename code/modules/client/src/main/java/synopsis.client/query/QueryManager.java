package synopsis.client.query;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.msg.client.ClientQueryRequest;
import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import org.apache.log4j.Logger;
import synopsis.client.ClientException;
import synopsis.client.query.tasks.ClientQueryRespHandlingTask;
import synopsis.client.query.tasks.QuerySubmitTask;
import synopsis.client.query.tasks.TargetQueryResponseHandlingTask;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Thilina Buddhika
 */
public class QueryManager {

    private final Logger logger = Logger.getLogger(QueryManager.class);
    private static QueryManager instance;
    private static final long QUERY_COUNT = 1000000;
    private long queryId;
    private final long maximumQueryId;
    private final String clientAddr;
    private ExecutorService executors = Executors.newFixedThreadPool(4);
    private boolean dispatcherModeEnabled = false;
    private Map<Long, QClient> dispatcherMap = new ConcurrentHashMap<>();

    private QueryManager(String hostAddr, int port) {
        this.queryId = getQueryIdOffSet(port);
        this.maximumQueryId = queryId + 2 * QUERY_COUNT;
        this.clientAddr = hostAddr + ":" + port;
    }

    public static synchronized QueryManager getInstance(String hostAddr, int port) {
        if (instance == null) {
            instance = new QueryManager(hostAddr, port);
        }
        return instance;
    }

    public void setDispatcherModeEnabled(boolean dispatcherModeEnabled) {
        this.dispatcherModeEnabled = dispatcherModeEnabled;
    }

    public long submitQuery(byte[] query, List<String> geoHashes, QueryCallback callback, String randomNodeAddr)
            throws ClientException {
        long queryId = getNextQueryId();
        ClientQueryRequest clientQueryReq = new ClientQueryRequest(queryId, this.clientAddr, query, geoHashes);
        QuerySubmitTask querySubmitTask = new QuerySubmitTask(queryId, randomNodeAddr, clientQueryReq, callback);
        executors.submit(querySubmitTask);
        return queryId;
    }

    private long getQueryIdOffSet(int port) {
        // this should be good enough to generate non-overlapping query id range for clients
        // in a distributed setup
        String hostAddr = "127.0.0.1";
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostAddr = inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            //logger.error("Error when retrieving the host address. Reverting to 127.0.0.1", e);
        }
        String hostAddrBasedQualifier = hostAddr.split("\\.")[3];
        String fullQualifier = hostAddrBasedQualifier + Integer.toString(port);
        return Long.parseLong(fullQualifier) * QUERY_COUNT;
    }

    synchronized long getNextQueryId() throws ClientException {
        if (++queryId <= maximumQueryId) {
            return queryId;
        } else {
            throw new ClientException("Query Id out of range. Current value: " + queryId + ", Max. Val: " +
                    maximumQueryId);
        }
    }

    public void handleClientQueryResponse(ClientQueryResponse clientQueryResponse) {
        if (this.dispatcherModeEnabled) {
            dispatch(clientQueryResponse.getQueryId(), clientQueryResponse);
        } else {
            this.executors.submit(new ClientQueryRespHandlingTask(clientQueryResponse));
        }
    }

    public void handleTargetQueryResponse(TargetQueryResponse targetQueryResponse) {
        if (this.dispatcherModeEnabled) {
            dispatch(targetQueryResponse.getQueryId(), targetQueryResponse);
        } else {
            this.executors.submit(new TargetQueryResponseHandlingTask(targetQueryResponse));
        }
    }

    synchronized void registerQClient(QClient client, long queryId) {
        dispatcherMap.put(queryId, client);
        if (logger.isDebugEnabled()) {
            logger.debug("QClient registered. Query Id: " + queryId);
        }
    }

    synchronized void removeClient(long queryId) {
        dispatcherMap.remove(queryId);
        if (logger.isDebugEnabled()) {
            logger.debug("QClient removed. Query Id: " + queryId);
        }
    }

    private void dispatch(Long queryId, ControlMessage ctrlMsg) {
        QClient client;
        synchronized (this) {
            client = dispatcherMap.get(queryId);
        }
        if (client != null) {
            client.handleQueryResponse(ctrlMsg);
        } else {
            logger.warn("No QClient available. Query id: " + queryId);
        }
    }

}
