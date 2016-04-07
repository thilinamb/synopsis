package neptune.geospatial.core.resource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.StreamSerializer;
import ds.granules.Granules;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.communication.direct.dispatch.ChannelToStreamDeMultiplexer;
import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.scheduler.Resource;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ParamsReader;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.msg.scaleout.DeploymentAck;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.StateTransferCompleteAck;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastNodeInstanceHolder;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Improves the behavior of <code>Resource</code> by
 * adding support for control messages. This will allow
 * communication with the job deployer as well as other
 * Resources.
 *
 * @author Thilina Buddhika
 */
public class ManagedResource {

    class MonitoredComputationState {
        private AbstractGeoSpatialStreamProcessor computation;
        private AtomicReference<ArrayList<Long>> backLogHistory = new AtomicReference<>(
                new ArrayList<Long>(monitoredBackLogLength));
        private AtomicBoolean eligibleForScaling = new AtomicBoolean(true);

        private MonitoredComputationState(AbstractGeoSpatialStreamProcessor computation) {
            this.computation = computation;
        }

        private double monitor() {
            long currentBacklog = computation.getBacklogLength();
            backLogHistory.get().add(currentBacklog);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Monitoring computation: %s. Adding a backlog: %d",
                        computation.getInstanceIdentifier(), currentBacklog));
            }
            while (backLogHistory.get().size() > monitoredBackLogLength) {
                backLogHistory.get().remove(0);
            }
            return isBacklogDeveloping();
        }

        private double isBacklogDeveloping() {
            double excess = 0;
            if (backLogHistory.get().size() >= monitoredBackLogLength) {
                boolean increasing = true;
                boolean decreasing = true;
                for (int i = 0; i < backLogHistory.get().size(); i++) {
                    double entry = backLogHistory.get().get(i);
                    if (entry < scaleOutThreshold) {
                        increasing = false;
                    }
                    if (entry > scaleInThreshold) {
                        decreasing = false;
                    }
                }
                if (increasing) {
                    excess = getAverageBacklog() - scaleOutThreshold;
                } else if (decreasing) {
                    excess = getAverageBacklog() - scaleInThreshold;
                }
            }
            return excess;
        }

        private double getAverageBacklog() {
            long sum = 0;
            for (long entry : backLogHistory.get()) {
                sum += entry;
            }
            return (sum * 1.0) / backLogHistory.get().size();
        }
    }

    /**
     * Monitors the computations to detect the stragglers.
     */
    private class ComputationMonitor implements Runnable {
        @Override
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("Monitoring thread is executing.");
            }
            try {
                synchronized (monitoredProcessors) {
                    // wait till computations are registered. avoid busy waiting at the beginning.
                    if (monitoredProcessors.isEmpty()) {
                        monitoredProcessors.wait();
                    }
                    for (String identifier : monitoredProcessors.keySet()) {
                        MonitoredComputationState monitoredComputationState = monitoredProcessors.get(identifier);
                        if (monitoredComputationState.eligibleForScaling.get()) {
                            double excess = monitoredComputationState.monitor();
                            if (excess != 0) {
                                // trigger scale up
                                boolean success = monitoredComputationState.computation.recommendScaling(excess);
                                monitoredComputationState.eligibleForScaling.set(!success);
                            }
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("Error in running computation monitor task.", e);
            }
        }
    }


    private static Logger logger = Logger.getLogger(ManagedResource.class.getName());
    // config properties
    private static final String ENABLE_DYNAMIC_SCALING = "rivulet-enable-dynamic-scaling";
    private static final String MONITORING_INTERVAL = "rivulet-monitor-interval";
    private static final String SCALE_OUT_THRESHOLD = "rivulet-scale-out-threshold";
    private static final String SCALE_IN_THRESHOLD = "rivulet-scale-in-threshold";
    private static final String MONITORED_BACKLOG_HISTORY_LENGTH = "rivulet-monitored-backlog-history-length";
    private static final String HAZELCAST_SERIALIZER_PREFIX = "rivulet-hazelcast-serializer-";
    private static final String HAZELCAST_INTERFACE = "rivulet-hazelcast-interface";
    public static final String ENABLE_FAULT_TOLERANCE = "rivulet-enable-fault-tolerance";
    private static final String STATE_REPLICATION_INTERVAL = "rivulet-state-replication-interval";

    // default values
    private int monitoredBackLogLength;
    private long scaleOutThreshold;
    private long scaleInThreshold;

    private static ManagedResource instance;
    private String deployerEndpoint = null;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private final Map<String, MonitoredComputationState> monitoredProcessors = new HashMap<>();
    private ScheduledExecutorService monitoringService = Executors.newSingleThreadScheduledExecutor();
    private Map<String, List<StateTransferMsg>> pendingStateTransfers = new HashMap<>();
    private Map<String, ScaleOutLockRequest> pendingScaleOutLockRequests = new HashMap<>();

    private boolean enableFaultTolerance = false;
    private long stateReplicationInterval = 2000;

    private ManagedResource(Properties inProps, int numOfThreads) throws CommunicationsException {
        Resource resource = new Resource(inProps, numOfThreads);
        resource.init();
        logger.info("Successfully Started ManagedResource.");
    }

    private void init() {
        instance = this;
        // register the callbacks to receive interference related control messages.
        AbstractProtocolHandler protoHandler = new ResourceProtocolHandler(this);
        ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, protoHandler);
        new Thread(protoHandler).start();
        try {
            Properties startupProps = NeptuneRuntime.getInstance().getProperties();
            deployerEndpoint = startupProps.getProperty(Constants.DEPLOYER_ENDPOINT);

            boolean enableDynamicScaling = startupProps.containsKey(ENABLE_DYNAMIC_SCALING) &&
                    Boolean.parseBoolean(startupProps.getProperty(ENABLE_DYNAMIC_SCALING));
            logger.info("Dynamic Scaling is " + (enableDynamicScaling ? "Enabled." : "Disabled"));
            // if dynamic scaling is available
            if (enableDynamicScaling) {
                // read dynamic scaling related configurations
                scaleOutThreshold = startupProps.containsKey(SCALE_OUT_THRESHOLD) ?
                        Integer.parseInt(startupProps.getProperty(SCALE_OUT_THRESHOLD)) : 100;
                scaleInThreshold = startupProps.containsKey(SCALE_IN_THRESHOLD) ?
                        Integer.parseInt(startupProps.getProperty(SCALE_IN_THRESHOLD)) : 10;
                int monitoringPeriod = startupProps.containsKey(MONITORING_INTERVAL) ?
                        Integer.parseInt(startupProps.getProperty(MONITORING_INTERVAL)) : 5000;
                monitoredBackLogLength = startupProps.containsKey(MONITORED_BACKLOG_HISTORY_LENGTH) ?
                        Integer.parseInt(startupProps.getProperty(MONITORED_BACKLOG_HISTORY_LENGTH)) : 5;
                // start the computation monitor thread
                monitoringService.scheduleWithFixedDelay(new ComputationMonitor(), 0, monitoringPeriod,
                        TimeUnit.MILLISECONDS);
                logger.info(String.format("Scale-in Threshold: %d, Scale-out Threshold: %d, " +
                                "Monitoring Period: %d (ms), Monitored Backlog History Length: %d", scaleInThreshold,
                        scaleOutThreshold, monitoringPeriod, monitoredBackLogLength));
            }
            // if fault tolerance enabled
            enableFaultTolerance = startupProps.containsKey(ENABLE_FAULT_TOLERANCE) && Boolean.parseBoolean(
                    startupProps.getProperty(ENABLE_FAULT_TOLERANCE).toLowerCase());
            if (enableFaultTolerance) {
                stateReplicationInterval = startupProps.containsKey(STATE_REPLICATION_INTERVAL) ?
                        Long.parseLong(startupProps.getProperty(STATE_REPLICATION_INTERVAL)) : 2000;
            }

            logger.info("Fault tolerance enabled: " + enableFaultTolerance);

            initializeHazelcast(startupProps);
            // register callback to receive deployment acks.
            ChannelToStreamDeMultiplexer.getInstance().registerCallback(Constants.DEPLOYMENT_REQ,
                    new DeployerCallback(this));
            countDownLatch.await();
        } catch (GranulesConfigurationException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static ManagedResource getInstance() throws NIException {
        if (instance == null) {
            throw new NIException("ManagedResource is not initialized.");
        }
        return instance;
    }

    private void initializeHazelcast(Properties startupProps) {
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
        }
        // set the logging framework
        config.setProperty("hazelcast.logging.type", "log4j");
        clientConfig.setProperty("hazelcast.logging.type", "log4j");
        HazelcastNodeInstanceHolder.init(config);
        HazelcastClientInstanceHolder.init(clientConfig);
        try {
            IMap map = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance().getMap(
                    GeoHashPrefixTree.PREFIX_MAP);
            // TODO: Remove me after the micro benchmark
            //map.addEntryListener(new GeoHashPrefixTree(), true);
            map.addEntryListener(new GeoHashPrefixTree(), true);
        } catch (neptune.geospatial.hazelcast.HazelcastException e) {
            logger.error("Error getting the Hazelcast client to register the entry listener.", e);
        }
    }

    public static void main(String[] args) {
        ParamsReader paramsReader = Granules.getParamsReader();
        String configLocation = "conf/ResourceConfig.txt";

        if (args.length != 0) {
            configLocation = args[0];
        }

        try {
            Properties resourceProps = new Properties();

            /* Read properties from config file, if it exists. */
            File configFile = new File(configLocation);
            if (configFile.exists()) {
                resourceProps = paramsReader.getProperties(configLocation);
            }

            /* Use System properties, if available. These overwrite values
             * specified in the config file. */
            String[] propNames = new String[]{Constants.NUM_THREADS_ENV_VAR,
                    Constants.FUNNEL_BOOTSTRAP_ENV_VAR,
                    Constants.DIRECT_COMM_LISTENER_PORT,
                    Constants.IO_REACTOR_THREAD_COUNT,
                    Constants.DIRECT_COMM_LISTENER_PORT};

            for (String propName : propNames) {
                String propertyValue = System.getProperty(propName);

                if (propertyValue != null) {
                    resourceProps.setProperty(propName, propertyValue);
                }
            }

            NeptuneRuntime.initialize(resourceProps);

            int numOfThreads = Integer.parseInt(
                    resourceProps.getProperty(Constants.NUM_THREADS_ENV_VAR, "4"));

            ManagedResource resource = new ManagedResource(resourceProps, numOfThreads);
            resource.init();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    void acknowledgeCtrlMsgListenerStartup() {
        countDownLatch.countDown();
    }

    public void sendToDeployer(ControlMessage controlMessage) {
        try {
            SendUtility.sendControlMessage(deployerEndpoint, controlMessage);
        } catch (CommunicationsException | IOException e) {
            logger.error("Error sending control message to the deployer.", e);
        }
    }

    public void registerStreamProcessor(AbstractGeoSpatialStreamProcessor processor) {
        synchronized (this) {
            String instanceIdentifier = processor.getInstanceIdentifier();
            monitoredProcessors.put(instanceIdentifier, new MonitoredComputationState(processor));
            if (pendingStateTransfers.containsKey(instanceIdentifier)) {
                List<StateTransferMsg> stateTransferMsgs = pendingStateTransfers.remove(instanceIdentifier);
                for (StateTransferMsg stateTransferMsg : stateTransferMsgs) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("Handing over the state for prefix: %s to: %s",
                                instanceIdentifier, stateTransferMsg.getPrefix()));
                    }
                    stateTransferMsg.setAcked(true);
                    processor.processCtrlMessage(stateTransferMsg);
                }
            }
            if (pendingScaleOutLockRequests.containsKey(instanceIdentifier)) {
                processor.processCtrlMessage(pendingScaleOutLockRequests.remove(instanceIdentifier));
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Handing over ScaleOutLockRequest to %s.", instanceIdentifier));
                }
            }
        }
        synchronized (monitoredProcessors) {
            monitoredProcessors.notifyAll();
        }
    }

    public void scalingOperationComplete(String computationIdentifier) {
        MonitoredComputationState monitoredComputationState = monitoredProcessors.get(computationIdentifier);
        monitoredComputationState.backLogHistory.get().clear();
        monitoredComputationState.eligibleForScaling.set(true);
    }

    void dispatchControlMessage(String computationId, ControlMessage ctrlMessage) {
        synchronized (this) {
            if (monitoredProcessors.containsKey(computationId)) {
                monitoredProcessors.get(computationId).computation.processCtrlMessage(ctrlMessage);
            } else if (ctrlMessage instanceof StateTransferMsg) {
                StateTransferMsg stateTransferMsg = (StateTransferMsg) ctrlMessage;
                if (!stateTransferMsg.isScaleType()) {
                    try {
                        List<StateTransferMsg> stateTransferMsgs = pendingStateTransfers.get(computationId);
                        if (stateTransferMsgs == null) {
                            stateTransferMsgs = new ArrayList<>();
                            pendingStateTransfers.put(computationId, stateTransferMsgs);
                        }
                        stateTransferMsgs.add(stateTransferMsg);
                        StateTransferCompleteAck completeAck = new StateTransferCompleteAck(stateTransferMsg.getKeyPrefix(),
                                stateTransferMsg.getPrefix(), stateTransferMsg.getOriginComputation());
                        SendUtility.sendControlMessage(stateTransferMsg.getOriginEndpoint(), completeAck);
                        logger.debug("New Computation is not active yet. Storing the StateTransfer Request.");
                    } catch (CommunicationsException | IOException e) {
                        logger.error("Error sending out the ScaleOutCompleteAck to " + stateTransferMsg.getOriginEndpoint());
                    }
                } else {
                    logger.warn("Invalid StateTransferMsg to " + computationId);
                }
            } else if (ctrlMessage instanceof ScaleOutLockRequest) {
                pendingScaleOutLockRequests.put(computationId, (ScaleOutLockRequest) ctrlMessage);
                if (logger.isDebugEnabled()) {
                    logger.debug("New computation is not active yet. Storing ScaleOutLockRequest.");
                }
            } else {
                logger.warn(String.format("Invalid control message to computation : %s, type: %d", computationId,
                        ctrlMessage.getMessageType()));
            }
        }
    }

    void ackDeployment(String instanceId) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending deployment ack for %s", instanceId));
        }
        sendToDeployer(new DeploymentAck(instanceId));
    }

    public boolean isFaultToleranceEnabled() {
        return this.enableFaultTolerance;
    }

    public long getStateReplicationInterval() {
        return stateReplicationInterval;
    }
}
