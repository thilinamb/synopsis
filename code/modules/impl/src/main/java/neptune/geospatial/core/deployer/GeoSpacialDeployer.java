package neptune.geospatial.core.deployer;

import ds.granules.communication.direct.JobDeployer;
import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.communication.direct.netty.server.MessageReceiver;
import ds.granules.communication.direct.netty.server.ServerHandler;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.DeploymentException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.exception.MarshallingException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.neptune.interfere.core.NIStreamProcessor;
import ds.granules.neptune.interfere.core.NIStreamSource;
import ds.granules.neptune.interfere.core.schedule.NIRoundRobinScheduler;
import ds.granules.neptune.interfere.core.schedule.NIScheduler;
import ds.granules.operation.Operation;
import ds.granules.operation.ProgressTracker;
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
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Customized deployer which can handle the dynamic scaling.
 *
 * @author Thilina Buddhika
 */
public class GeoSpacialDeployer extends JobDeployer {

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

    private static final Logger logger = Logger.getLogger(JobDeployer.class);
    private CountDownLatch initializationLatch = new CountDownLatch(2);
    private NIScheduler scheduler = new NIRoundRobinScheduler();
    private static GeoSpacialDeployer instance;
    private Map<String, String> niOpAssignments = new HashMap<>();
    private Map<String, Operation> niOperationsMap = new HashMap<>();
    private Map<String, String> niOperationToJobId = new HashMap<>();
    private Map<String, String> niControlToDataEndPoints = new HashMap<>();
    //  destination -> source
    private Map<String, String> niSourceDestinationMap = new HashMap<>();


    @Override
    public ProgressTracker deployOperations(List<Operation[]> ops) throws CommunicationsException, DeploymentException, MarshallingException {
        String source = null;
        String destination = null;
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
            niOperationsMap.put(operatorId, op);
            niOperationToJobId.put(operatorId, jobId);
            niControlToDataEndPoints.put(resourceEndpoint.getControlEndpoint(), resourceEndpoint.getDataEndpoint());
            try {
                // write the assignments to ZooKeeper
                ZooKeeperUtils.createDirectory(zk, Constants.ZK_ZNODE_OP_ASSIGNMENTS + "/" + operatorId,
                        resourceEndpoint.getDataEndpoint().getBytes(), CreateMode.PERSISTENT);
                if (op instanceof NIStreamSource) {
                    source = operatorId;
                } else if (op instanceof NIStreamProcessor) {
                    destination = operatorId;
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error(e.getMessage(), e);
                throw new DeploymentException(e.getMessage(), e);
            }
        }

        // send the deployment messages
        for (Map.Entry<Operation, String> entry : assignments.entrySet()) {
            deployOperation(jobId, entry.getValue(), entry.getKey());
        }

        niSourceDestinationMap.put(destination, source);
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

    public synchronized static GeoSpacialDeployer getDeployer() throws NIException {
        if (instance == null) {
            instance = new GeoSpacialDeployer();
            try {
                instance.initialize(NeptuneRuntime.getInstance().getProperties());
            } catch (CommunicationsException | IOException | DeploymentException | MarshallingException | GranulesConfigurationException e) {
                logger.error("Error initializing NIJobDeployer", e);
                throw new NIException(e.getMessage(), e);
            }
        }
        return instance;
    }

    protected void ackCtrlMsgHandlerStarted(){
        initializationLatch.countDown();
    }

    private void initializationCompleted() {
        try {
            initializationLatch.await();
        } catch (InterruptedException ignore) {

        }
    }
}
