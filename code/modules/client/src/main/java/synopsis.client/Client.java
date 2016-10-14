package synopsis.client;

import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ZooKeeperUtils;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import synopsis.client.messaging.ClientProtocolHandler;
import synopsis.client.messaging.Transport;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryManager;

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
    private final QueryManager queryManager;
    private final Random random = new Random();

    public Client(Properties properties, int clientPort) throws ClientException {
        try {
            NeptuneRuntime.initialize(properties);
            this.endpoints = new ArrayList<>();
            this.zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
            this.clientPort = clientPort;
            this.hostname = RivuletUtil.getHostInetAddress().getHostName();
            queryManager = QueryManager.getInstance(this.hostname, this.clientPort);
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
                    this.endpoints.add(new SynopsisEndpoint(segments[0], Integer.parseInt(segments[2])));
                }
            }
        } catch (KeeperException | InterruptedException e) {
            throw new ClientException("Error in resource discovery.", e);
        }
        logger.info("Discovered " + this.endpoints.size() + " resources.");
    }

    public long submitQuery(byte[] query, List<String> geoHashes, QueryCallback callback) throws ClientException {
        return queryManager.submitQuery(query, geoHashes, callback, getRandomSynopsisNode());
    }

    private String getRandomSynopsisNode(){
        SynopsisEndpoint endpoint = endpoints.get(random.nextInt(endpoints.size()));
        return endpoint.toString();
    }
}
