package neptune.geospatial.util;

import ds.funnel.topic.Topic;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ZooKeeperUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class RivuletUtil {

    private static final Logger logger = Logger.getLogger(RivuletUtil.class);

    /**
     * Get the {@code InetAddress} of the current host
     * @return @code InetAddress} of the current host
     */
    public static InetAddress getHostInetAddress() {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        return inetAddr;
    }

    /**
     * Returns the control plain address of the current process
     * @return Control plain address
     * @throws GranulesConfigurationException Error reading the configuration file
     */
    public static String getCtrlEndpoint() throws GranulesConfigurationException {
        try {
            return getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(
                            Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT);
        } catch (GranulesConfigurationException e) {
            logger.error("Error when retrieving the hostname.", e);
            throw e;
        }
    }

    public static String getResourceEndpointForTopic(ZooKeeper zk, Topic topic) throws KeeperException, InterruptedException {
        String topicPath = ds.granules.util.Constants.ZK_ZNODE_STREAMS + "/" + topic.toString();
        List<String> subscribers = ZooKeeperUtils.getChildDirectories(zk, topicPath);
        if (subscribers != null) {
            // for the first subscriber, get their deployed locations -> there can be only one subscriber as per our scaling model
            String subscriberId = subscribers.get(0).substring(
                    subscribers.get(0).lastIndexOf("/") + 1, subscribers.get(0).length());
            String zNodePath = ds.granules.util.Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + subscriberId;
            byte[] endPointData = ZooKeeperUtils.readZNodeData(zk, zNodePath);
            return new String(endPointData);

        }
        return null;
    }
}
