package neptune.geospatial.core.resource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.StreamSerializer;
import ds.granules.Granules;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
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
import neptune.geospatial.core.protocol.msg.*;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastNodeInstanceHolder;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
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
                    } else if (entry > scaleInThreshold) {
                        decreasing = false;
                    }
                }
                if (increasing) {
                    excess = backLogHistory.get().get(backLogHistory.get().size() - 1) - scaleOutThreshold;
                } else if (decreasing) {
                    excess = backLogHistory.get().get(backLogHistory.get().size() - 1) - scaleInThreshold;
                }
            }
            return excess;
        }
    }

    /**
     * Monitors the computations to detect the stragglers.
     */
    class ComputationMonitor implements Runnable {
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
    public static final String ENABLE_DYNAMIC_SCALING = "rivulet-enable-dynamic-scaling";
    public static final String MONITORING_INTERVAL = "rivulet-monitor-interval";
    public static final String SCALE_OUT_THRESHOLD = "rivulet-scale-out-threshold";
    public static final String SCALE_IN_THRESHOLD = "rivulet-scale-in-threshold";
    public static final String MONITORED_BACKLOG_HISTORY_LENGTH = "rivulet-monitored-backlog-history-length";
    public static final String HAZELCAST_SERIALIZER_PREFIX = "rivulet-hazelcast-serializer-";
    public static final String HAZELCAST_INTERFACE = "rivulet-hazelcast-interface";

    // default values
    public int monitoredBackLogLength;
    public long scaleOutThreshold;
    public long scaleInThreshold;
    public int monitoringPeriod;

    private static ManagedResource instance;
    private String deployerEndpoint = null;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private final Map<String, MonitoredComputationState> monitoredProcessors = new HashMap<>();
    private ScheduledExecutorService monitoringService = Executors.newSingleThreadScheduledExecutor();
    private volatile Map<String, StateTransferMsg> pendingStateTransfers = new ConcurrentHashMap<>();

    public ManagedResource(Properties inProps, int numOfThreads) throws CommunicationsException {
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
                monitoringPeriod = startupProps.containsKey(MONITORING_INTERVAL) ?
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
            initializeHazelcast(startupProps);
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
        if (startupProps.containsKey(HAZELCAST_INTERFACE)){
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

    protected void acknowledgeCtrlMsgListenerStartup() {
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
        synchronized (monitoredProcessors) {
            monitoredProcessors.put(processor.getInstanceIdentifier(), new MonitoredComputationState(processor));
            if (pendingStateTransfers.containsKey(processor.getInstanceIdentifier())) {
                StateTransferMsg stateTransferMsg = pendingStateTransfers.remove(processor.getInstanceIdentifier());
                logger.debug(String.format("Handing over the state for prefix: %s to: %s",
                        processor.getInstanceIdentifier(), stateTransferMsg.getPrefix()));
                processor.handleStateTransferReq(stateTransferMsg, true);
            }
            monitoredProcessors.notifyAll();
        }
    }

    public void scalingOperationComplete(String computationIdentifier) {
        MonitoredComputationState monitoredComputationState = monitoredProcessors.get(computationIdentifier);
        monitoredComputationState.backLogHistory.get().clear();
        monitoredComputationState.eligibleForScaling.set(true);
    }

    public void handleTriggerScaleAck(ScaleOutResponse ack) {
        String processorId = ack.getTargetComputation();
        if (monitoredProcessors.containsKey(processorId)) {
            monitoredProcessors.get(processorId).computation.handleTriggerScaleAck(ack);
        } else {
            logger.warn("ScaleTriggerAck for an invalid computation: " + processorId);
        }
    }


    public void handleScaleOutCompleteAck(ScaleOutCompleteAck ack) {
        String processorId = ack.getTargetComputation();
        if (monitoredProcessors.containsKey(processorId)) {
            monitoredProcessors.get(processorId).computation.handleScaleOutCompleteAck(ack);
        } else {
            logger.warn("ScaleOutCompleteAck for an invalid computation: " + processorId);
        }
    }

    public void handleScaleInLockReq(ScaleInLockRequest lockReq) {
        String computationId = lockReq.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInLockReq(lockReq);
        } else {
            logger.warn("Invalid ScaleInLockReq for computation: " + computationId);
        }
    }

    public void handleScaleInLockResp(ScaleInLockResponse lockResponse) {
        String computationId = lockResponse.getComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInLockResponse(lockResponse);
        } else {
            logger.warn("Invalid ScaleInLockResponse for computation: " + computationId);
        }
    }

    public void handleScaleInActivateReq(ScaleInActivateReq activateReq) {
        String computationId = activateReq.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInActivateReq(activateReq);
        } else {
            logger.warn("Invalid ScaleInLockReq for computation: " + computationId);
        }
    }

    public void handleStateTransferMsg(StateTransferMsg stateTransferMsg) {
        String computationId = stateTransferMsg.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleStateTransferReq(stateTransferMsg, false);
        } else {
            if (!stateTransferMsg.isScaleType()) {
                try {
                    pendingStateTransfers.put(computationId, stateTransferMsg);
                    ScaleOutCompleteAck completeAck = new ScaleOutCompleteAck(stateTransferMsg.getKeyPrefix(),
                            stateTransferMsg.getPrefix(), stateTransferMsg.getOriginComputation());
                    SendUtility.sendControlMessage(stateTransferMsg.getOriginEndpoint(), completeAck);
                    logger.debug("New Computation is not active yet. Storing the StateTransfer Request.");
                } catch (CommunicationsException | IOException e) {
                    logger.error("Error sending out the ScaleOutCompleteAck to " + stateTransferMsg.getOriginEndpoint());
                }
            } else {
                logger.warn("Invalid StateTransferMsg to " + computationId);
            }
        }
    }

    public void handleScaleInCompleteMsg(ScaleInComplete completeMsg) {
        String computationId = completeMsg.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInCompleteMsg(completeMsg);
        } else {
            logger.warn("Invalid ScaleInComplete msg to " + computationId);
        }
    }

    public void handleScaleInCompleteAckMsg(ScaleInCompleteAck ack) {
        String computationId = ack.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInCompleteAck(ack);
        } else {
            logger.warn("Invalid ScaleInComplete msg to " + computationId);
        }
    }
}
