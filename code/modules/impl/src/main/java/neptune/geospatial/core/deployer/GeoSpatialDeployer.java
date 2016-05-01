package neptune.geospatial.core.deployer;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import ds.funnel.topic.StringTopic;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.JobDeployer;
import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.communication.direct.netty.server.MessageReceiver;
import ds.granules.communication.direct.netty.server.ServerHandler;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.DeploymentException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.exception.MarshallingException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.operation.Operation;
import ds.granules.operation.ProcessingException;
import ds.granules.operation.ProgressTracker;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.StreamSource;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ZooKeeperUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.msg.scaleout.DeploymentAck;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutRequest;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutResponse;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.ft.StateReplicaProcessor;
import neptune.geospatial.ft.TopicInfo;
import neptune.geospatial.ft.protocol.StateReplicationLevelIncreaseMsg;
import neptune.geospatial.ft.zk.MembershipChangeListener;
import neptune.geospatial.ft.zk.MembershipTracker;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Customized deployer which can handle the dynamic scaling.
 *
 * @author Thilina Buddhika
 */
public class GeoSpatialDeployer extends JobDeployer implements MembershipChangeListener {

    private class CtrlMessageEndpoint implements Runnable {

        private int ctrlPort;

        CtrlMessageEndpoint(int ctrlPort) {
            this.ctrlPort = ctrlPort;
        }

        @Override
        public void run() {

            // launch the control plane port
            EventLoopGroup ctrlBossGroup = new NioEventLoopGroup(1);
            EventLoopGroup ctrlWorkerGroup = new NioEventLoopGroup(1);

            ServerBootstrap ctrlBootstrap = new ServerBootstrap();
            ctrlBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            ctrlBootstrap.group(ctrlBossGroup, ctrlWorkerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new MessageReceiver(),
                                    new ServerHandler(ControlMessageDispatcher.getInstance()));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            // bind to the control plane port
            try {
                ctrlBootstrap.bind(this.ctrlPort).sync();
                initializationLatch.countDown();
                logger.info("Control Message Endpoint is initialized for deployer on port " + this.ctrlPort);
            } catch (InterruptedException e) {
                logger.error("Error starting the ctrl message endpoint. ", e);
            }
        }
    }

    private static final Logger logger = Logger.getLogger(GeoSpatialDeployer.class);
    private static final String DEPLOYER_CONFIG_PATH = "deployer-config";

    private CountDownLatch initializationLatch = new CountDownLatch(2);
    private static GeoSpatialDeployer instance;
    private Map<String, Operation> computationIdToObjectMap = new HashMap<>();
    private Map<String, Topic> stateReplicationOpPlacements = new HashMap<>();
    private Map<String, Topic> resourceEndpointToDefaultTopicMap = new HashMap<>();
    private List<ScaleOutResponse> pendingDeployments = new ArrayList<>();
    private String jobId;
    private DeployerConfig deployerConfig;
    private boolean faultToleranceEnabled;
    private Map<TopicInfo, TopicInfo[]> stateReplicationTopics = new HashMap<>();
    private ResourceMonitor resourceMonitor;

    @Override
    public ProgressTracker deployOperations(Operation[] operations) throws
            CommunicationsException, DeploymentException, MarshallingException {
        this.jobId = uuidRetriever.getRandomBasedUUIDAsString();

        if (faultToleranceEnabled) {
            // deploy state replica computations at each resource
            deployStateReplicaOperators(jobId);
        }
        AbstractGeoSpatialStreamProcessor original = null;
        // shuffle operations
        List<Operation> opList = Arrays.asList(operations);
        //Collections.shuffle(opList);
        opList.toArray(operations);

        if (!resourceEndpoints.isEmpty()) {
            try {
                List<ResourceEndpoint> layerOneEndpoints = new ArrayList<>();
                Map<Operation, String> assignments = new HashMap<>(operations.length);
                // create the deployment plan
                for (Operation op : operations) {
                    ResourceEndpoint resourceEndpoint = nextResource(op);
                    // keep track of the layer one endpoints to skip them from scheduling for scaling out
                    if (op instanceof AbstractGeoSpatialStreamProcessor) {
                        layerOneEndpoints.add(resourceEndpoint);
                    } else if (op instanceof StreamSource){
                        layerOneEndpoints.add(resourceEndpoint);
                    }
                    assignments.put(op, resourceEndpoint.getDataEndpoint());
                    String operatorId = op.getInstanceIdentifier();
                    computationIdToObjectMap.put(operatorId, op);
                    // write the assignments to ZooKeeper
                    ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + operatorId,
                            resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
                    // configure state replication
                    if (faultToleranceEnabled && op instanceof AbstractGeoSpatialStreamProcessor) {
                        AbstractGeoSpatialStreamProcessor geoSpatialStreamProcessor = (AbstractGeoSpatialStreamProcessor) op;
                        configureReplicationStreams(geoSpatialStreamProcessor, lastAssigned - 1);
                        resourceEndpointToDefaultTopicMap.put(resourceEndpoint.getDataEndpoint(),
                                geoSpatialStreamProcessor.getDefaultGeoSpatialStream());
                        if (original == null) {
                            original = (AbstractGeoSpatialStreamProcessor) op;
                        }
                    }
                }
                // initialize the resource monitor
                if (resourceMonitor == null) {
                    resourceMonitor = new ResourceMonitor(resourceEndpoints, layerOneEndpoints);
                    resourceMonitor.init();
                }

                // deploy an empty computation at every node, so that it can take over if the primary fails
                if (faultToleranceEnabled && original != null) {
                    for (ResourceEndpoint resourceEndpoint : resourceEndpoints) {
                        if (!resourceEndpointToDefaultTopicMap.containsKey(resourceEndpoint.getDataEndpoint())) {
                            try {
                                // copy the minimal state
                                AbstractGeoSpatialStreamProcessor clone = original.getClass().newInstance();
                                original.createMinimalClone(clone);
                                // create incoming topics
                                Topic topic = deployGeoSpatialProcessor(resourceEndpoint.getDataEndpoint(), clone);
                                assignments.put(clone, resourceEndpoint.getDataEndpoint());
                                configureReplicationStreams(clone, resourceEndpoints.indexOf(resourceEndpoint));
                                if (topic != null) {
                                    resourceEndpointToDefaultTopicMap.put(resourceEndpoint.getDataEndpoint(), topic);
                                }
                                if (logger.isDebugEnabled()) {
                                    logger.debug(String.format("An additional computation is deployed on %s",
                                            resourceEndpoint.getDataEndpoint()));
                                }
                            } catch (Exception e) {
                                String errMsg = "Error deploying additional computation at resource %s" +
                                        resourceEndpoint.getDataEndpoint();
                                logger.error(errMsg, e);
                                throw new DeploymentException(errMsg, e);
                            }
                        }
                    }
                    // create the zookeeeper backup node hierarchy
                    createZKBackupProcessorMap(assignments);
                }

                // send the deployment messages
                for (Map.Entry<Operation, String> entry : assignments.entrySet()) {
                    deployOperation(jobId, entry.getValue(), entry.getKey());
                }

            } catch (KeeperException | InterruptedException e) {
                logger.error(e.getMessage(), e);
                throw new DeploymentException(e.getMessage(), e);
            } catch (HazelcastException e) {
                throw new DeploymentException("Error initializing Resource Monitor", e);
            }
        } else {
            logger.error("Zero Granules Resources Discovered. Terminating the deployment.");
            System.exit(-1);
        }
        return null;
    }

    private void configureReplicationStreams(AbstractGeoSpatialStreamProcessor streamProcessor, int location) {
        Topic[] topics = new Topic[]{stateReplicationOpPlacements.get(
                resourceEndpoints.get((location + 1) % resourceEndpoints.size()).getDataEndpoint())
                , stateReplicationOpPlacements.get(
                resourceEndpoints.get((location + 2) % resourceEndpoints.size()).getDataEndpoint())};
        streamProcessor.deployStateReplicationStreams(topics);
        // keep track of the backup topics for later
        TopicInfo[] stateReplicationTopics = {new TopicInfo(topics[0],
                resourceEndpoints.get((location + 1) % resourceEndpoints.size()).getDataEndpoint()),
                new TopicInfo(topics[1],
                        resourceEndpoints.get((location + 2) % resourceEndpoints.size()).getDataEndpoint())};
        this.stateReplicationTopics.put(new TopicInfo(streamProcessor.getDefaultGeoSpatialStream(),
                resourceEndpoints.get(location).getDataEndpoint()), stateReplicationTopics);
        // set the state replications stream topics
        streamProcessor.setReplicationStreamTopics(Arrays.asList(stateReplicationTopics));
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("State Replication Processors are deployed for %s in %s and %s",
                    streamProcessor.getInstanceIdentifier(),
                    resourceEndpoints.get((lastAssigned + 1) % resourceEndpoints.size()).getDataEndpoint(),
                    resourceEndpoints.get((lastAssigned + 2) % resourceEndpoints.size()).getDataEndpoint()));
        }
    }

    private void deployStateReplicaOperators(String jobId) throws CommunicationsException, DeploymentException {
        ZooKeeper zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
        // deploy one state replica operator per instance
        for (ResourceEndpoint resourceEndpoint : resourceEndpoints) {
            StateReplicaProcessor stateReplicaProcessor = new StateReplicaProcessor();
            int topicId;
            try {
                topicId = ZooKeeperUtils.getNextSequenceNumber(zk);
                Topic topic = new StringTopic(Integer.toString(topicId));
                // create a subscription
                stateReplicaProcessor.registerIncomingTopic(topic, jobId);
                // write the assignments to ZooKeeper
                ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" +
                                stateReplicaProcessor.getInstanceIdentifier(),
                        resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
                // deploy the operator
                deployOperation(jobId, resourceEndpoint.getDataEndpoint(), stateReplicaProcessor);
                stateReplicationOpPlacements.put(resourceEndpoint.getDataEndpoint(), topic);
            } catch (KeeperException | InterruptedException | StreamingDatasetException e) {
                throw new DeploymentException(e.getMessage(), e);
            }
        }
    }

    private Topic deployGeoSpatialProcessor(String endpoint, StreamProcessor clone)
            throws CommunicationsException, DeploymentException {
        ZooKeeper zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
        Topic topic;
        try {
            int topicId = ZooKeeperUtils.getNextSequenceNumber(zk);
            topic = new StringTopic(Integer.toString(topicId));
            ((AbstractGeoSpatialStreamProcessor) clone).registerDefaultTopic(topic);
            // write the assignments to ZooKeeper
            ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + clone.getInstanceIdentifier(),
                    endpoint.getBytes(), CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException | StreamingDatasetException e) {
            throw new DeploymentException(e.getMessage(), e);
        }
        return topic;
    }

    private ResourceEndpoint nextResource(Operation op) {
        if (deployerConfig != null) {
            String resourceEndpointUrl = deployerConfig.getPlacementNode(op.getClass().getName());
            if (resourceEndpointUrl != null) {
                logger.info("Custom placement config is available for " + op.getClass().getName());
                String[] addrSegments = resourceEndpointUrl.split(":");
                return new ResourceEndpoint(addrSegments[0], Integer.parseInt(addrSegments[1]),
                        Integer.parseInt(addrSegments[2]));
            }
        }
        ResourceEndpoint resourceEndpoint = resourceEndpoints.get(lastAssigned);
        lastAssigned = (lastAssigned + 1) % resourceEndpoints.size();
        return resourceEndpoint;
    }

    @Override
    public void initialize(Properties streamingProperties) throws CommunicationsException, IOException, MarshallingException, DeploymentException {
        int ctrlPort = Integer.parseInt(streamingProperties.getProperty(
                Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT));
        new Thread(new CtrlMessageEndpoint(ctrlPort)).start();
        DeployerProtocolHandler protocolHandler = new DeployerProtocolHandler(this);
        new Thread(protocolHandler).start();
        ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, protocolHandler);
        // config is provided
        if (streamingProperties.containsKey(DEPLOYER_CONFIG_PATH)) {
            String configPath = streamingProperties.getProperty(DEPLOYER_CONFIG_PATH);
            Reader configReader = new FileReader(configPath);
            Gson gson = new Gson();
            JsonReader jsonReader = gson.newJsonReader(configReader);
            deployerConfig = gson.fromJson(jsonReader, DeployerConfig.class);
            logger.info("Using the custom deployment configuration.");
        }
        if (streamingProperties.containsKey(ManagedResource.ENABLE_FAULT_TOLERANCE)) {
            faultToleranceEnabled = Boolean.parseBoolean(streamingProperties.getProperty(ManagedResource.ENABLE_FAULT_TOLERANCE));
        }
        logger.info("Fault Tolerance Enabled: " + faultToleranceEnabled);
        if (faultToleranceEnabled) {
            MembershipTracker.getInstance().registerListener(this);
        }
        RivuletUtil.initializeHazelcast(streamingProperties);
        super.initialize(streamingProperties);
        initializationCompleted();
    }

    public synchronized static GeoSpatialDeployer getDeployer() throws NIException {
        if (instance == null) {
            instance = new GeoSpatialDeployer();
            try {
                instance.initialize(NeptuneRuntime.getInstance().getProperties());
            } catch (CommunicationsException | IOException | DeploymentException | MarshallingException | GranulesConfigurationException e) {
                logger.error("Error initializing NIJobDeployer", e);
                throw new NIException(e.getMessage(), e);
            }
        }
        return instance;
    }

    void ackCtrlMsgHandlerStarted() {
        initializationLatch.countDown();
    }

    private void initializationCompleted() {
        try {
            initializationLatch.await();
            logger.info("Deployer Initialization is complete. Ready to launch jobs.");
        } catch (InterruptedException ignore) {

        }
    }

    void handleScaleUpRequest(ScaleOutRequest scaleOutReq) throws GeoSpatialDeployerException {
        String computationId = scaleOutReq.getCurrentComputation();
        if (!computationIdToObjectMap.containsKey(computationId)) {
            throw new GeoSpatialDeployerException("Invalid computation: " + computationId);
        }
        StreamProcessor currentComp = (StreamProcessor) computationIdToObjectMap.get(computationId);
        ScaleOutResponse ack = null;
        try {
            StreamProcessor clone = currentComp.getClass().newInstance();
            // copy the minimal state
            currentComp.createMinimalClone(clone);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Successfully created a minimal clone. Current Computation: %s, " +
                        "New Computation: %s", computationId, clone.getInstanceIdentifier()));
            }
            currentComp.addStreamConsumer(new StringTopic(scaleOutReq.getTopic()), clone, scaleOutReq.getStreamId(),
                    scaleOutReq.getStreamType());
            // deploy
            ResourceEndpoint resourceEndpoint = resourceMonitor.assignResource(scaleOutReq.getRequiredMemory(),
                    scaleOutReq.getOriginEndpoint());

            // not sufficient resources available
            if (resourceEndpoint == null) {
                ScaleOutResponse resp = new ScaleOutResponse(scaleOutReq.getMessageId(), computationId, false,
                        scaleOutReq.getPrefixOnlyScaleOutOperationId());
                try {
                    SendUtility.sendControlMessage(scaleOutReq.getOriginEndpoint(), resp);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending out the TriggerScaleAck to " + scaleOutReq.getOriginEndpoint());
                }
                return;
            }

            logger.info(String.format("%s[%s] --> %s[%s]", scaleOutReq.getCurrentComputation(),
                    scaleOutReq.getOriginEndpoint(), clone.getInstanceIdentifier(), resourceEndpoint.getDataEndpoint()));

            // initialize the state replication streams for the new computation
            if (faultToleranceEnabled && clone instanceof AbstractGeoSpatialStreamProcessor) {
                AbstractGeoSpatialStreamProcessor streamProcessor = (AbstractGeoSpatialStreamProcessor) clone;
                configureReplicationStreams(streamProcessor, lastAssigned == 0 ? resourceEndpoints.size() - 1 :
                        lastAssigned - 1);
                createBackupTopicZNode(streamProcessor, resourceEndpoint.getDataEndpoint());
            }
            // write the assignments to ZooKeeper
            ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" +
                            clone.getInstanceIdentifier(),
                    resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
            // deploy the operation in the chosen Granules resource
            computationIdToObjectMap.put(clone.getInstanceIdentifier(), clone);
            ack = new ScaleOutResponse(scaleOutReq.getMessageId(), computationId, scaleOutReq.getOriginEndpoint(), true,
                    clone.getInstanceIdentifier(), resourceEndpoint.getControlEndpoint(), scaleOutReq.getPrefixOnlyScaleOutOperationId());
            pendingDeployments.add(ack);
            deployOperation(this.jobId, resourceEndpoint.getDataEndpoint(), clone);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Sent the deployment request new instance. Instance Id: %s, Location: %s",
                        clone.getInstanceIdentifier(), resourceEndpoint.getDataEndpoint()));
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GeoSpatialDeployerException("Error instantiating the new copy of the computation.", e);
        } catch (ProcessingException | StreamingGraphConfigurationException | StreamingDatasetException e) {
            throw new GeoSpatialDeployerException("Error copying minimal state from the current computation.", e);
        } catch (KeeperException | InterruptedException e) {
            throw new GeoSpatialDeployerException("Error writing the deployment data to ZK.", e);
        } finally {
            if (ack == null) {
                ack = new ScaleOutResponse(scaleOutReq.getMessageId(), computationId, false, scaleOutReq.getPrefixOnlyScaleOutOperationId());
                try {
                    SendUtility.sendControlMessage(scaleOutReq.getOriginEndpoint(), ack);
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending out the TriggerScaleAck to " + scaleOutReq.getOriginEndpoint());
                }
            }
        }
    }

    void handleDeploymentAck(DeploymentAck deploymentAck) {
        if (!pendingDeployments.isEmpty()) {
            ScaleOutResponse ack = pendingDeployments.remove(0);
            try {
                SendUtility.sendControlMessage(ack.getTargetEndpoint(), ack);
                if (logger.isDebugEnabled()) {
                    logger.debug("Sent the ScaleOutResponse after processing deployment ack for " + ack.getTargetComputation());
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending out the TriggerScaleAck to " + ack.getTargetEndpoint());
            }
        } else {
            logger.warn("Invalid Deployment Ack. No pending deployments.");
        }
    }

    private void createZKBackupProcessorMap(Map<Operation, String> assignments) throws DeploymentException {
        try {
            if (!ZooKeeperUtils.directoryExists(zk, neptune.geospatial.graph.Constants.ZNodes.ZNODE_BACKUP_TOPICS)) {
                ZooKeeperUtils.createDirectory(zk, neptune.geospatial.graph.Constants.ZNodes.ZNODE_BACKUP_TOPICS, null,
                        CreateMode.PERSISTENT);
            }
            for (Map.Entry<Operation, String> entry : assignments.entrySet()) {
                Operation op = entry.getKey();
                if (op instanceof AbstractGeoSpatialStreamProcessor) {
                    createBackupTopicZNode((AbstractGeoSpatialStreamProcessor) op, entry.getValue());
                }
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error creating Zookeeper backup node tree.", e);
            throw new DeploymentException("Error creating Zookeeper backup node tree.", e);
        }
    }

    private void createBackupTopicZNode(AbstractGeoSpatialStreamProcessor processor, String resourceEndpoint)
            throws KeeperException, InterruptedException {
        Topic defaultGSSTopic = processor.getDefaultGeoSpatialStream();
        TopicInfo defaultTopicInfo = new TopicInfo(defaultGSSTopic, resourceEndpoint);
        if (stateReplicationTopics.containsKey(defaultTopicInfo)) {
            TopicInfo[] stateReplicationTopics = this.stateReplicationTopics.get(defaultTopicInfo);
            String backupNodePath = neptune.geospatial.graph.Constants.ZNodes.ZNODE_BACKUP_TOPICS + "/" +
                    defaultGSSTopic.toString();
            ZooKeeperUtils.createDirectory(zk, backupNodePath, null, CreateMode.PERSISTENT);
            for (TopicInfo stateReplicationTopic : stateReplicationTopics) {
                Topic processorTopic = resourceEndpointToDefaultTopicMap.get(stateReplicationTopic.getResourceEndpoint());
                String childNodePath = backupNodePath + "/" + processorTopic.toString();
                ZooKeeperUtils.createDirectory(zk, childNodePath, stateReplicationTopic.getResourceEndpoint().getBytes(),
                        CreateMode.PERSISTENT);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("ZK Backup Node tree updated. Topic: %s [%s], " +
                                    "Backup Node: %s [%s]", defaultGSSTopic, resourceEndpoint,
                            processorTopic, stateReplicationTopic.getResourceEndpoint()));
                }
            }
        } else {
            logger.error("No backup topics found for " + defaultGSSTopic);
        }
    }

    @Override
    public void membershipChanged(List<String> lostMembers) {
        if (logger.isDebugEnabled()) {
            logger.debug("Detected lost members. Evaluating under replicated nodes. Lost Node Count: " +
                    lostMembers.size());
        }
        // first update the resource endpoint list
        Map<String, ResourceEndpoint> repIndex = reIndexResourceList(lostMembers);
        List<TopicInfo> toBeRemoved = new ArrayList<>();

        for (Map.Entry<TopicInfo, TopicInfo[]> entry : stateReplicationTopics.entrySet()) {
            TopicInfo gsProcessorTopicInfo = entry.getKey();
            String primaryLoc = gsProcessorTopicInfo.getResourceEndpoint();
            if (lostMembers.contains(primaryLoc)) {
                toBeRemoved.add(gsProcessorTopicInfo);
                continue;
            }
            TopicInfo[] stateReplicationTopics = entry.getValue();
            for (int i = 0; i < stateReplicationTopics.length; i++) {
                TopicInfo stateReplicationTopic = stateReplicationTopics[i];
                String replicationEndpoint = stateReplicationTopic.getResourceEndpoint();
                if (lostMembers.contains(replicationEndpoint)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("Detected a under replicated topic. " +
                                        "Primary processor location: %s, Failed replication endpoint: %s",
                                gsProcessorTopicInfo.getResourceEndpoint(), replicationEndpoint));
                    }
                    // update the zookeeper
                    String invalidZKPath = neptune.geospatial.graph.Constants.ZNodes.ZNODE_BACKUP_TOPICS + "/" +
                            gsProcessorTopicInfo.getTopic() + "/" + resourceEndpointToDefaultTopicMap.get(replicationEndpoint);
                    try {
                        zk.delete(invalidZKPath, -1);
                        // find a new location
                        int currentLocationIndex = resourceEndpoints.indexOf(repIndex.get(primaryLoc));
                        String newLocation = getLocationForNextReplica(stateReplicationTopics, currentLocationIndex);
                        Topic newStateReplicationTopic = stateReplicationOpPlacements.get(newLocation);
                        Topic newBackupProcessorTopic = resourceEndpointToDefaultTopicMap.get(newLocation);
                        TopicInfo newStateReplicationTopicInfo = new TopicInfo(newStateReplicationTopic, newLocation);
                        stateReplicationTopics[i] = newStateReplicationTopicInfo;
                        String newZkPath = neptune.geospatial.graph.Constants.ZNodes.ZNODE_BACKUP_TOPICS + "/" +
                                gsProcessorTopicInfo.getTopic() + "/" + newBackupProcessorTopic.toString();
                        ZooKeeperUtils.createDirectory(zk, newZkPath, newLocation.getBytes(),
                                CreateMode.PERSISTENT);
                        if (logger.isDebugEnabled()) {
                            logger.info(String.format("Replication level increased. New State Replication Topic: %s " +
                                            "New Processor Topic: %s New Location: %s", newStateReplicationTopic.toString(),
                                    newBackupProcessorTopic.toString(), newLocation));
                        }
                        // send a message to the computation with new state replication topic info
                        String targetComputation = RivuletUtil.getSubscriberComputationForTopic(zk, gsProcessorTopicInfo.getTopic());
                        StateReplicationLevelIncreaseMsg repLevelIncreaseMsg = new StateReplicationLevelIncreaseMsg(targetComputation,
                                newLocation, newStateReplicationTopic.toString());
                        SendUtility.sendControlMessage(repIndex.get(primaryLoc).getControlEndpoint(), repLevelIncreaseMsg);
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("Sent a StateReplicationLevelIncreaseMsg to %s. " +
                                            "Old topic: %s New topic: %s", repIndex.get(primaryLoc).getControlEndpoint(),
                                    stateReplicationTopic.getTopic().toString(),
                                    newStateReplicationTopic.toString()));
                        }
                    } catch (InterruptedException | KeeperException e) {
                        logger.error("Error updating replication topic in Zookeeper.", e);
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending StateReplicationLevelIncreaseMsg message to " +
                                repIndex.get(primaryLoc).getControlEndpoint());
                    }
                }
            }
        }
        for (TopicInfo invalidItem : toBeRemoved) {
            stateReplicationTopics.remove(invalidItem);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Cleaning up topics from lost nodes. Topic: %s Endpoint: %s",
                        invalidItem.getTopic(), invalidItem.getResourceEndpoint()));
            }
        }
    }

    private Map<String, ResourceEndpoint> reIndexResourceList(List<String> lostMembers) {
        List<ResourceEndpoint> newResourceList = new ArrayList<>(resourceEndpoints.size() - lostMembers.size());
        Map<String, ResourceEndpoint> index = new HashMap<>();
        for (ResourceEndpoint endpoint : resourceEndpoints) {
            if (!lostMembers.contains(endpoint.getDataEndpoint())) {
                newResourceList.add(endpoint);
                index.put(endpoint.getDataEndpoint(), endpoint);
            } else if (logger.isDebugEnabled()) {
                logger.debug(String.format("Lost Resource endpoint detected. data endpoint: %s", endpoint.getDataEndpoint()));
            }
        }
        resourceEndpoints = newResourceList;
        return index;
    }

    private String getLocationForNextReplica(TopicInfo[] currentTopics, int currentIndex) {
        List<String> currentLocations = new ArrayList<>(currentTopics.length);
        for (TopicInfo topicInfo : currentTopics) {
            currentLocations.add(topicInfo.getResourceEndpoint());
        }
        // let's start with a new location with a reasonable chance, because we use a replication level of 2
        int nextIndex = currentIndex + 2;
        String nextLoc = resourceEndpoints.get((nextIndex) % resourceEndpoints.size()).getDataEndpoint();
        while (currentLocations.contains(nextLoc)) {
            nextLoc = resourceEndpoints.get((++nextIndex) % resourceEndpoints.size()).getDataEndpoint();
        }
        return nextLoc;
    }
}
