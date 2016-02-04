package neptune.geospatial.core.deployer;

import ds.funnel.topic.StringTopic;
import ds.granules.communication.direct.JobDeployer;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.communication.direct.netty.server.MessageReceiver;
import ds.granules.communication.direct.netty.server.ServerHandler;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.DeploymentException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.exception.MarshallingException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.neptune.interfere.core.schedule.NIRoundRobinScheduler;
import ds.granules.neptune.interfere.core.schedule.NIScheduler;
import ds.granules.operation.Operation;
import ds.granules.operation.ProcessingException;
import ds.granules.operation.ProgressTracker;
import ds.granules.streaming.core.StreamProcessor;
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
import neptune.geospatial.core.protocol.msg.TriggerScale;
import neptune.geospatial.core.protocol.msg.TriggerScaleAck;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Customized deployer which can handle the dynamic scaling.
 *
 * @author Thilina Buddhika
 */
public class GeoSpatialDeployer extends JobDeployer {

    private class CtrlMessageEndpoint implements Runnable {

        private int ctrlPort;

        public CtrlMessageEndpoint(int ctrlPort) {
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
    private CountDownLatch initializationLatch = new CountDownLatch(2);
    private NIScheduler scheduler = new NIRoundRobinScheduler();
    private static GeoSpatialDeployer instance;
    private Map<String, String> niOpAssignments = new HashMap<>();
    private Map<String, Operation> computationIdToObjectMap = new HashMap<>();
    private Map<String, String> niControlToDataEndPoints = new HashMap<>();
    private String jobId;

    @Override
    public ProgressTracker deployOperations(Operation[] operations) throws
            CommunicationsException, DeploymentException, MarshallingException {
        // shuffle operations
        List<Operation> opList = Arrays.asList(operations);
        //Collections.shuffle(opList);
        opList.toArray(operations);

        this.jobId = uuidRetriever.getRandomBasedUUIDAsString();
        try {
            Map<Operation, String> assignments = new HashMap<>(operations.length);
            // create the deployment plan
            for (Operation op : operations) {
                ResourceEndpoint resourceEndpoint = nextResource();
                assignments.put(op, resourceEndpoint.getDataEndpoint());
                String operatorId = op.getInstanceIdentifier();
                niOpAssignments.put(operatorId, resourceEndpoint.getControlEndpoint());
                computationIdToObjectMap.put(operatorId, op);
                niControlToDataEndPoints.put(resourceEndpoint.getControlEndpoint(), resourceEndpoint.getDataEndpoint());
                // write the assignments to ZooKeeper
                ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + operatorId,
                        resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
            }
            // send the deployment messages
            for (Map.Entry<Operation, String> entry : assignments.entrySet()) {
                deployOperation(jobId, entry.getValue(), entry.getKey());
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new DeploymentException(e.getMessage(), e);
        }
        return null;
    }

    private ResourceEndpoint nextResource() {
        ResourceEndpoint resourceEndpoint = resourceEndpoints.get(lastAssigned);
        lastAssigned = (lastAssigned + 1) % resourceEndpoints.size();
        return resourceEndpoint;
    }

    @Override
    public ProgressTracker deployOperations(List<Operation[]> ops) throws CommunicationsException, DeploymentException, MarshallingException {
        String jobId = uuidRetriever.getRandomBasedUUIDAsString();
        Map<Operation, String> assignments = new HashMap<>();
        Map<Operation, ResourceEndpoint> deploymentPlan = scheduler.schedule(ops, resourceEndpoints);
        // process each group separately
        for (Operation op : deploymentPlan.keySet()) {
            ResourceEndpoint resourceEndpoint = deploymentPlan.get(op);
            assignments.put(op, resourceEndpoint.getDataEndpoint());
            // store them for migration by the NIResource deployer
            String operatorId = op.getInstanceIdentifier();
            niOpAssignments.put(operatorId, resourceEndpoint.getControlEndpoint());
            computationIdToObjectMap.put(operatorId, op);
            niControlToDataEndPoints.put(resourceEndpoint.getControlEndpoint(), resourceEndpoint.getDataEndpoint());
            try {
                // write the assignments to ZooKeeper
                ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + operatorId,
                        resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
            } catch (KeeperException | InterruptedException e) {
                logger.error(e.getMessage(), e);
                throw new DeploymentException(e.getMessage(), e);
            }
        }

        // send the deployment messages
        for (Map.Entry<Operation, String> entry : assignments.entrySet()) {
            deployOperation(jobId, entry.getValue(), entry.getKey());
        }

        return null;
    }

    @Override
    public void initialize(Properties streamingProperties) throws CommunicationsException, IOException, MarshallingException, DeploymentException {
        int ctrlPort = Integer.parseInt(streamingProperties.getProperty(
                Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT));
        new Thread(new CtrlMessageEndpoint(ctrlPort)).start();
        DeployerProtocolHandler protocolHandler = new DeployerProtocolHandler(this);
        new Thread(protocolHandler).start();
        ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, protocolHandler);
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

    protected void ackCtrlMsgHandlerStarted() {
        initializationLatch.countDown();
    }

    private void initializationCompleted() {
        try {
            initializationLatch.await();
            logger.info("Deployer Initialization is complete. Ready to launch jobs.");
        } catch (InterruptedException ignore) {

        }
    }

    public void handleScaleUpRequest(TriggerScale scaleOutReq) throws GeoSpatialDeployerException {
        String computationId = scaleOutReq.getCurrentComputation();
        if (!computationIdToObjectMap.containsKey(computationId)) {
            throw new GeoSpatialDeployerException("Invalid computation: " + computationId);
        }
        StreamProcessor currentComp = (StreamProcessor) computationIdToObjectMap.get(computationId);
        boolean success = false;
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
            ResourceEndpoint resourceEndpoint = nextResource();
            // write the assignments to ZooKeeper
            ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" +
                            clone.getInstanceIdentifier(),
                    resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
            // deploy the operation in the chosen Granules resource
            deployOperation(this.jobId, resourceEndpoint.getDataEndpoint(), clone);
            success = true;
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Successfully deployed the new instance. Instance Id: %s, Location: %s",
                        clone.getInstanceIdentifier(), resourceEndpoint.getDataEndpoint()));
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GeoSpatialDeployerException("Error instantiating the new copy of the computation.", e);
        } catch (ProcessingException | StreamingGraphConfigurationException | StreamingDatasetException e) {
            throw new GeoSpatialDeployerException("Error copying minimal state from the current computation.", e);
        } catch (KeeperException | InterruptedException e) {
            throw new GeoSpatialDeployerException("Error writing the deployment data to ZK.", e);
        } finally {
            TriggerScaleAck ack = new TriggerScaleAck(scaleOutReq.getMessageId(), success);
            try {
                SendUtility.sendControlMessage(scaleOutReq.getOriginEndpoint(), ack);
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending out the TriggerScaleAck to " + scaleOutReq.getOriginEndpoint());
            }
        }
    }
}
