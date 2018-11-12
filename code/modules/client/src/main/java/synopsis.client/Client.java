package synopsis.client;

import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ZooKeeperUtils;
import neptune.geospatial.core.protocol.msg.client.PersistStateRequest;
import neptune.geospatial.ft.zk.MembershipTracker;
import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import synopsis.client.failuredetection.FailureMonitor;
import synopsis.client.messaging.ClientProtocolHandler;
import synopsis.client.messaging.Transport;
import synopsis.client.persistence.PersistenceCompletionCallback;
import synopsis.client.persistence.PersistenceManager;
import synopsis.client.query.PrefixMemUsageQueryCallback;
import synopsis.client.query.QClient;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Synopsis Client implementation
 *
 * @author Thilina Buddhika
 */
public class Client {

    private final Logger logger = Logger.getLogger(Client.class);
    private final ZooKeeper zk;
    private List<SynopsisEndpoint> endpoints;
    private final int clientPort;
    private final String hostname;
    protected final QueryManager queryManager;
    private FailureMonitor failureMonitor;
    private final Random random = new Random();

    public Client(Properties properties, int clientPort) throws ClientException {
        try {
            NeptuneRuntime.initialize(properties);
            this.endpoints = new ArrayList<>();
            this.zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
            this.clientPort = clientPort;
            this.hostname = RivuletUtil.getHostInetAddress().getHostName();
            queryManager = QueryManager.getInstance(this.hostname, this.clientPort);
            queryManager.setDispatcherModeEnabled(true);
        } catch (GranulesConfigurationException | CommunicationsException e) {
            throw new ClientException("Error in initializing. ", e);
        }
    }

    public void init() throws ClientException {
        try {
            // start the Client message dispatcher
            CountDownLatch dispatcherLatch = new CountDownLatch(1);
            ClientProtocolHandler messageDispatcher = new ClientProtocolHandler(dispatcherLatch, this.queryManager);
            new Thread(messageDispatcher).start();
            dispatcherLatch.await();
            ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, messageDispatcher);
            logger.info("Message dispatcher started!");
            // start the transport
            CountDownLatch transportLatch = new CountDownLatch(1);
            Transport transport = new Transport(this.clientPort, transportLatch);
            new Thread(transport).start();
            transportLatch.await();
            logger.info("Transport module is started!");
        } catch (InterruptedException e) {
            throw new ClientException("Error in initialization.", e);
        }
        // discover resources
        discoverSynopsisNodes();
        try {
            failureMonitor = new FailureMonitor(endpoints);
            MembershipTracker.getInstance().registerListener(failureMonitor);
        } catch (IOException e) {
            logger.error("Error instantiating the failure monitor.", e);
        } catch (CommunicationsException e) {
            logger.error(e.getMessage(), e);
        }
        logger.info("Client initialization is complete.");
    }

    private void discoverSynopsisNodes() throws ClientException {
        try {
            List childDirs = ZooKeeperUtils.getChildDirectories(zk, "/granules-cluster");
            Iterator iterator = childDirs.iterator();
            while (iterator.hasNext()) {
                String resource = (String) iterator.next();
                byte[] endpointData = ZooKeeperUtils.readZNodeData(this.zk, "/granules-cluster/" + resource);
                if (endpointData != null) {
                    String[] segments = (new String(endpointData)).split(":");
                    this.endpoints.add(new SynopsisEndpoint(segments[0], Integer.parseInt(segments[1]), Integer.parseInt(segments[2])));
                }
            }
        } catch (KeeperException | InterruptedException e) {
            throw new ClientException("Error in resource discovery.", e);
        }
        logger.info("Discovered " + this.endpoints.size() + " resources.");
    }

    protected long submitQuery(byte[] query, List<String> geoHashes, QueryCallback callback) throws ClientException {
        return queryManager.submitQuery(query, geoHashes, callback, getRandomSynopsisNode());
    }

    protected long submitQuery(long queryId, byte[] query, List<String> geoHashes, QueryCallback callback) throws ClientException {
        return queryManager.submitQuery(queryId, query, geoHashes, callback, getRandomSynopsisNode());
    }

    void launchQClients(int qClientCount, int queryCount, QueryWrapper[] queries,
                        QueryCreator.QueryType[] qTypes, double[] percentages) throws ClientException {
        CountDownLatch countDownLatch = new CountDownLatch(qClientCount);
        QClient[] qClients = new QClient[qClientCount];
        for (int i = 0; i < qClientCount; i++) {
            try {
                QClient qClient = new QClient(queryCount, queries, qTypes, percentages, this.getAddr(),
                        this.queryManager, this.endpoints, countDownLatch);
                qClients[i] = qClient;
                Thread clientThread = new Thread(qClient);
                clientThread.start();
            } catch (ClientException e) {
                logger.error("Error initializing the QClient.", e);
                throw e;
            }
        }
        onQClientsCompletion(countDownLatch, qClients);
    }

    void launchQClients(int qClientCount, int queryCount) {
        CountDownLatch countDownLatch = new CountDownLatch(qClientCount);
        QClient[] qClients = new QClient[qClientCount];
        for (int i = 0; i < qClientCount; i++) {
            QClient qClient = new QClient(queryCount, this.endpoints, this.getAddr(),
                    this.queryManager, countDownLatch);
            qClients[i] = qClient;
            Thread clientThread = new Thread(qClient);
            clientThread.start();
        }
        onQClientsCompletion(countDownLatch, qClients);
        Main.notifyOperationComplete();
    }

    private void onQClientsCompletion(CountDownLatch countDownLatch, QClient[] qClients) {
        try {
            countDownLatch.await();
            logger.info("All the QClient threads have completed. Merging results.");
            /*QClientStatRecorder statRecorder = new QClientStatRecorder();
            for (QClient qClient : qClients) {
                statRecorder.merge(qClient.getStatRecorder());
            }
            statRecorder.writeToFile("/tmp/" + hostname + "-" + clientPort + ".cstat");
            logger.info("Written merged results to file.");
            */
        } catch (InterruptedException ignore) {

        }
    }

    public void getMemConsumptionInfo(String[] prefixes) throws ClientException {
        long queryId = -1;
        PrefixMemUsageQueryCallback callback = new PrefixMemUsageQueryCallback(prefixes.length);
        for (String prefix : prefixes) {
            List<String> prefixList = new ArrayList<>();
            prefixList.add(prefix);
            queryId--;
            submitQuery(queryId, prefix.getBytes(), prefixList, callback);
        }
        logger.info("Submitted " + prefixes.length + " queries.");
    }

    void serializeState(PersistenceCompletionCallback cb) throws ClientException {
        long checkpointId = System.currentTimeMillis();
        PersistenceManager.getInstance().submitPersistenceTask(checkpointId, endpoints.size(), cb);
        for (SynopsisEndpoint endpoint : endpoints) {
            String synopsisEp = endpoint.getHostname() + ":" + endpoint.getControlPort();
            PersistStateRequest persistStateReq = new PersistStateRequest(checkpointId, getAddr(), true);
            try {
                System.out.println("Sent a serialization request to: " + synopsisEp);
                SendUtility.sendControlMessage(synopsisEp, persistStateReq);
            } catch (CommunicationsException | IOException e) {
                String eMsg = "Error sending the serialization request to the endpoint: " + synopsisEp;
                logger.error(eMsg, e);
                throw new ClientException(eMsg, e);
            }
        }
        try {
            Thread.sleep(5 * 60 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!cb.isCompleted()) {
            PersistenceManager.getInstance().forceCompletion(checkpointId);
        }
        Main.notifyOperationComplete();
    }

    void terminateNodes(){
        failureMonitor.triggerFailures();
    }

    public String getRandomSynopsisNode() {
        SynopsisEndpoint endpoint = endpoints.get(random.nextInt(endpoints.size()));
        return endpoint.getHostname() + ":" + endpoint.getControlPort();
    }

    private String getAddr() {
        return this.hostname + ":" + this.clientPort;
    }
}
