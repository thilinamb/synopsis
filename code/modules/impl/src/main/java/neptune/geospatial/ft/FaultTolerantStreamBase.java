package neptune.geospatial.ft;

import ds.funnel.topic.StringTopic;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.exception.CommunicationsException;
import ds.granules.streaming.core.StreamBase;
import ds.granules.util.ZooKeeperUtils;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public interface FaultTolerantStreamBase {

    Logger logger = Logger.getLogger(FaultTolerantStreamBase.class);

    default Map<String, List<BackupTopicInfo>> populateBackupTopicMap(String instanceIdentifier, Map<String,
            List<StreamBase.StreamDisseminationMetadata>> metadataRegistry) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Starting to populate backup topic map. " +
                            "Computation Type: %s, Metadata Registry Size: %d", instanceIdentifier,
                    this.getClass().toString(), metadataRegistry.size()));
        }
        Map<String, List<BackupTopicInfo>> topicLocations = new HashMap<>();
        try {
            ZooKeeper zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
            // 1. read the out-going topics from dissemination metadata
            for (String stream : metadataRegistry.keySet()) {
                processBackupTopicPerStream(zk, instanceIdentifier, stream, metadataRegistry, topicLocations);
            }
        } catch (CommunicationsException | InterruptedException | KeeperException e) {
            logger.error("Error when populating the backup topic map.", e);
        }
        return topicLocations;
    }

    default void processBackupTopicPerStream(ZooKeeper zk, String instanceIdentifier,
                                             String stream,
                                             Map<String, List<StreamBase.StreamDisseminationMetadata>> metadataRegistry,
                                             Map<String, List<BackupTopicInfo>> topicLocations)
            throws KeeperException, InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Processing backup topics for stream: %s",
                    instanceIdentifier, stream));
        }
        // skip the replication streams for now
        if (stream.startsWith(Constants.Streams.STATE_REPLICA_STREAM)) {
            return;
        }
        List<StreamBase.StreamDisseminationMetadata> metadataList = metadataRegistry.get(stream);
        for (StreamBase.StreamDisseminationMetadata metadata : metadataList) {
            // for each topic, find the backup topics
            for (Topic topic : metadata.topics) {
                // endpoint where the primary topic is running. We will be monitoring this topic
                String resourceEP = RivuletUtil.getResourceEndpointForTopic(zk, topic);
                List<TopicInfo> backupTopics = new ArrayList<>();
                // find the backup topics
                String backupNodePath = neptune.geospatial.graph.Constants.ZNodes.ZNODE_BACKUP_TOPICS + "/" +
                        topic.toString();
                if (ZooKeeperUtils.directoryExists(zk, backupNodePath)) {
                    List<String> backZNodePaths = ZooKeeperUtils.getChildDirectories(zk, backupNodePath);
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Backup topics found for topic :%s. " +
                                        "Backup topic count: %d", instanceIdentifier, topic.toString(),
                                backZNodePaths.size()));
                    }
                    for (String backupZNode : backZNodePaths) {
                        // location where the backup topic is running.
                        // this is required if a backup topic has become the primary
                        String backupResourceEP = new String(ZooKeeperUtils.readZNodeData(zk,
                                backupNodePath + "/" + backupZNode));
                        Topic backupTopic = new StringTopic(
                                backupZNode.substring(backupZNode.lastIndexOf("/") + 1, backupZNode.length()));
                        TopicInfo topicInfo = new TopicInfo(backupTopic, backupResourceEP);
                        backupTopics.add(topicInfo);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] Backup topic found: " +
                                            "Primary Topic: %s, Primary Topic Loc: %s, Backup Topic:%s, " +
                                            "Backup Topic Loc: %s", instanceIdentifier, topic.toString(),
                                    resourceEP, backupTopic.toString(), backupResourceEP));
                        }
                    }
                }
                // it is possible that single resource endpoints host multiple topics
                if (topicLocations.containsKey(resourceEP)) {
                    topicLocations.get(resourceEP).add(new BackupTopicInfo(stream, topic, backupTopics));
                } else {
                    List<BackupTopicInfo> backupTopicList = new ArrayList<>();
                    backupTopicList.add(new BackupTopicInfo(stream, topic, backupTopics));
                    topicLocations.put(resourceEP, backupTopicList);
                }
            }
        }
    }
}
