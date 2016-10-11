package synopsis.client;

import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ZooKeeperUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Synopsis Client implementation
 *
 * @author Thilina Buddhika
 */
public class Client {

    private final Logger logger = Logger.getLogger(Client.class);
    private final ZooKeeper zk;
    private List<SynopsisEndpoint> endpoints;

    public Client(Properties properties) throws ClientException {
        try {
            this.endpoints = new ArrayList<>();
            this.zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
            NeptuneRuntime.initialize(properties);
            init();
            logger.info("Client initialization is complete.");
        } catch (GranulesConfigurationException | CommunicationsException e) {
            throw new ClientException("Error in initializing. ", e);
        }
    }

    private void init() throws ClientException {
        // discover resources
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

}
