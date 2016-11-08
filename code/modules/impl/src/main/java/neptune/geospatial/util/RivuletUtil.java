package neptune.geospatial.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.nio.serialization.StreamSerializer;
import ds.funnel.topic.Topic;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ZooKeeperUtils;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastNodeInstanceHolder;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;

/**
 * @author Thilina Buddhika
 */
public class RivuletUtil {
    private static final String HAZELCAST_SERIALIZER_PREFIX = "rivulet-hazelcast-serializer-";
    private static final String HAZELCAST_INTERFACE = "rivulet-hazelcast-interface";

    private static final Logger logger = Logger.getLogger(RivuletUtil.class);

    /**
     * Get the {@code InetAddress} of the current host
     *
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
     *
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

    /**
     * Returns the control plain address of the current process
     *
     * @return Control plain address
     * @throws GranulesConfigurationException Error reading the configuration file
     */
    public static String getDataEndpoint() throws GranulesConfigurationException {
        try {
            return getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(
                            Constants.DIRECT_COMM_LISTENER_PORT);
        } catch (GranulesConfigurationException e) {
            logger.error("Error when retrieving the hostname.", e);
            throw e;
        }
    }

    public static String getResourceEndpointForTopic(ZooKeeper zk, Topic topic) throws KeeperException, InterruptedException {
        String subscriberId = getSubscriberComputationForTopic(zk, topic);
        String zNodePath = ds.granules.util.Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + subscriberId;
        byte[] endPointData = ZooKeeperUtils.readZNodeData(zk, zNodePath);
        return new String(endPointData);
    }

    public static String getSubscriberComputationForTopic(ZooKeeper zk, Topic topic) throws KeeperException, InterruptedException {
        String topicPath = ds.granules.util.Constants.ZK_ZNODE_STREAMS + "/" + topic.toString();
        List<String> subscribers = ZooKeeperUtils.getChildDirectories(zk, topicPath);
        // for the first subscriber, get their deployed locations -> there can be only one subscriber as per our scaling model
        return subscribers.get(0).substring(
                subscribers.get(0).lastIndexOf("/") + 1, subscribers.get(0).length());
    }

    public static void initializeHazelcast(Properties startupProps){
        Config config = new Config();
        ClientConfig clientConfig = new ClientConfig();
        for (String propName : startupProps.stringPropertyNames()) {
            if (propName.startsWith(HAZELCAST_SERIALIZER_PREFIX)) {
                String typeClazzName = propName.substring(HAZELCAST_SERIALIZER_PREFIX.length(), propName.length());
                String serializerClazzName = startupProps.getProperty(propName);
                try {
                    Class typeClazz = Class.forName(typeClazzName);
                    Class serializerClazz = Class.forName(serializerClazzName);
                    StreamSerializer serializer = (StreamSerializer) serializerClazz.newInstance();
                    SerializerConfig sc = new SerializerConfig().setImplementation(serializer).setTypeClass(typeClazz);
                    config.getSerializationConfig().addSerializerConfig(sc);
                    clientConfig.getSerializationConfig().addSerializerConfig(sc);
                    logger.info("Successfully Added Hazelcast Serializer for type " + typeClazzName);
                } catch (ClassNotFoundException e) {
                    logger.error("Error instantiating Type class through reflection. Class name: " + typeClazzName, e);
                } catch (InstantiationException | IllegalAccessException e) {
                    logger.error("Error creating a new instance of the serializer. Class name: " + serializerClazzName,
                            e);
                }
            }
        }
        // set the interfaces for Hazelcast to bind with.
        if (startupProps.containsKey(HAZELCAST_INTERFACE)) {
            String allowedInterface = startupProps.getProperty(HAZELCAST_INTERFACE);
            config.getNetworkConfig().getInterfaces().addInterface(allowedInterface).setEnabled(true);
            config.getNetworkConfig().getInterfaces().addInterface("129.82.*.*").setEnabled(true);
        }
        // set the logging framework
        config.setProperty("hazelcast.logging.type", "log4j");
        clientConfig.setProperty("hazelcast.logging.type", "log4j");
        HazelcastNodeInstanceHolder.init(config);
        HazelcastClientInstanceHolder.init(clientConfig);
    }

    public static double inGigabytes(double bytes){
        return bytes/(1024 * 1024 * 1024);
    }

    public static void writeByteArrayToDisk(byte[] bytes, String fileName){
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(fileName));
            dos.writeInt(bytes.length);
            dos.write(bytes);
            dos.flush();
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] readByteArrayFromDisk(String fileName){
        try {
            DataInputStream dis = new DataInputStream(new FileInputStream(fileName));
            int len = dis.readInt();
            byte[] bytes = new byte[len];
            dis.readFully(bytes);
            dis.close();
            return bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
