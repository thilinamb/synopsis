package neptune.geospatial.core.resource;

import com.hazelcast.core.IMap;
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
import neptune.geospatial.core.protocol.msg.client.ClientQueryRequest;
import neptune.geospatial.core.protocol.msg.client.ClientQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetedQueryRequest;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.msg.EnableShortCircuiting;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.core.protocol.msg.scaleout.DeploymentAck;
import neptune.geospatial.core.protocol.msg.scaleout.PrefixOnlyScaleOutCompleteAck;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.StateTransferCompleteAck;
import neptune.geospatial.ft.protocol.CheckpointAck;
import neptune.geospatial.graph.operators.NOAADataIngester;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.util.*;
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

    private class MonitoredComputationState {
        private AbstractGeoSpatialStreamProcessor computation;
        private AtomicReference<ArrayList<Long>> backLogHistory = new AtomicReference<>(
                new ArrayList<>(monitoredBackLogLength));
        private AtomicBoolean eligibleForScaling = new AtomicBoolean(true);

        private MonitoredComputationState(AbstractGeoSpatialStreamProcessor computation) {
            this.computation = computation;
        }

        private double[] update() {
            long currentBacklog = computation.getBacklogLength();
            backLogHistory.get().add(currentBacklog);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Monitoring computation: %s. Adding a backlog: %d",
                        computation.getInstanceIdentifier(), currentBacklog));
            }
            while (backLogHistory.get().size() > monitoredBackLogLength) {
                backLogHistory.get().remove(0);
            }
            return new double[]{isBacklogDeveloping(), getMemoryConsumption()};
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

        private double getMemoryConsumption() {
            return computation.getMemoryConsumptionForAllPrefixes();
        }

        private double getAverageBacklog() {
            long sum = 0;
            for (long entry : backLogHistory.get()) {
                sum += entry;
            }
            return (sum * 1.0) / backLogHistory.get().size();
        }
    }

    private class SortableMetric implements Comparable<SortableMetric> {
        private AbstractGeoSpatialStreamProcessor computation;
        private double value;

        private SortableMetric(AbstractGeoSpatialStreamProcessor computation, double value) {
            this.computation = computation;
            this.value = value;
        }

        @Override
        public int compareTo(SortableMetric o) {
            return -1 * new Double(this.value).compareTo(o.value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SortableMetric that = (SortableMetric) o;

            return computation.getInstanceIdentifier().equals(that.computation.getInstanceIdentifier());

        }

        @Override
        public int hashCode() {
            return computation.getInstanceIdentifier().hashCode();
        }
    }

    /**
     * Monitors the computations to detect the stragglers.
     */
    private class ComputationMonitor implements Runnable {

        private AtomicBoolean eligibleForScaling = new AtomicBoolean(true);
        private List<Long> memoryConsumption = new ArrayList<>(monitoredBackLogLength);
        private int counter;

        @Override
        public void run() {
            // terminate the monitoring task after the active scaling period is elapsed, if it's set.
            if (activeScalingPeriod > 0) {
                long now = System.currentTimeMillis();
                if (tsActiveScalingStarted == 0) {
                    tsActiveScalingStarted = now;
                } else if ((now - tsActiveScalingStarted) >= activeScalingPeriod) {
                    logger.info("Active Scaling Period is elapsed. Terminating the monitoring task.");
                    future.cancel(false);
                    return;
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Monitoring thread is executing.");
            }

            if (counter == 0) {
                double currAvailableMem = getAvailableMem();
                if (memoryUsageMap != null) {
                    memoryUsageMap.put(dataEndpoint, currAvailableMem);
                }
            }
            double avgMemoryUsage = updateMemoryConsumption();

            try {
                synchronized (monitoredProcessors) {
                    // wait till computations are registered. avoid busy waiting at the beginning.
                    if (monitoredProcessors.isEmpty()) {
                        monitoredProcessors.wait();
                    }
                    counter = ++counter % 5;
                    // update each monitored computation
                    Queue<SortableMetric> backlogs = new PriorityQueue<>();
                    Queue<SortableMetric> memoryUsage = new PriorityQueue<>();
                    for (String identifier : monitoredProcessors.keySet()) {
                        MonitoredComputationState monitoredComputationState = monitoredProcessors.get(identifier);
                        double[] metrics = monitoredComputationState.update();
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("[%s] - backlog: %.3f, mem. consumption: %.3f", identifier, metrics[0], metrics[1]));
                        }
                        backlogs.add(new SortableMetric(monitoredComputationState.computation, metrics[0]));
                        memoryUsage.add(new SortableMetric(monitoredComputationState.computation, metrics[1]));
                            /*if (excess != 0) {
                                // trigger scale up
                                boolean success = monitoredComputationState.computation.recommendScaling(excess);
                                monitoredComputationState.eligibleForScaling.set(!success);
                            }*/
                    }
                    if (!eligibleForScaling.get()) {
                        return;
                    }
                    if (avgMemoryUsage >= 0.5 && memoryUsage.size() > 0) {
                        SortableMetric highestMemConsumer = memoryUsage.poll();
                        double excessMemUsage = getExcessMemConsumption(avgMemoryUsage);
                        logger.info(String.format("Chosen for scaling: Mode: MEMORY, Computation: %s, Mem. usage: %.3f, Excess. mem. usage: %s",
                                highestMemConsumer.computation.getInstanceIdentifier(), highestMemConsumer.value, excessMemUsage));
                        boolean success = highestMemConsumer.computation.recommendScaling(excessMemUsage, true);
                        eligibleForScaling.set(!success);
                    } else if (backlogs.size() > 0) {
                        SortableMetric longestBacklog = backlogs.poll();
                        while (longestBacklog.value == 0 && !backlogs.isEmpty()) {
                            longestBacklog = backlogs.poll();
                        }
                        if (longestBacklog.value != 0) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(String.format("Chosen for scaling: Mode: BACKLOG, Computation: %s, Backlog: %.3f ",
                                        longestBacklog.computation.getInstanceIdentifier(), longestBacklog.value));
                            }
                            boolean success = longestBacklog.computation.recommendScaling(longestBacklog.value, false);
                            eligibleForScaling.set(!success);
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("Error in running computation monitor task.", e);
            }
        }

        private long getAvailableMem() {
            MemoryUsage memUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            if (memUsage.getUsed() * 1.0 / memUsage.getMax() > 0.5) {
                logger.info(String.format("Max mem: %.3f, Used Mem: %.3f, Available Mem: %.3f ",
                        RivuletUtil.inGigabytes(memUsage.getMax()),
                        RivuletUtil.inGigabytes(memUsage.getUsed()),
                        RivuletUtil.inGigabytes(memUsage.getMax() -
                                memUsage.getUsed())));
            }
            return (memUsage.getMax() - memUsage.getUsed());
        }

        private double getExcessMemConsumption(double avgMemConsumption) {
            MemoryUsage memUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            return (avgMemConsumption - 0.4) * memUsage.getMax();
        }


        private double updateMemoryConsumption() {
            MemoryUsage memUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            memoryConsumption.add(memUsage.getUsed());
            if (memoryConsumption.size() > monitoredBackLogLength) {
                memoryConsumption = memoryConsumption.subList(memoryConsumption.size() - monitoredBackLogLength,
                        memoryConsumption.size());
            }
            double sum = 0.0;
            for (long reading : memoryConsumption) {
                sum += reading;
            }
            return (sum / memoryConsumption.size()) / memUsage.getMax();
        }
    }


    private static Logger logger = Logger.getLogger(ManagedResource.class.getName());
    // config properties
    private static final String ENABLE_DYNAMIC_SCALING = "rivulet-enable-dynamic-scaling";
    private static final String MONITORING_INTERVAL = "rivulet-monitor-interval";
    private static final String SCALE_OUT_THRESHOLD = "rivulet-scale-out-threshold";
    private static final String SCALE_IN_THRESHOLD = "rivulet-scale-in-threshold";
    private static final String MONITORED_BACKLOG_HISTORY_LENGTH = "rivulet-monitored-backlog-history-length";
    public static final String ENABLE_FAULT_TOLERANCE = "rivulet-enable-fault-tolerance";
    private static final String STATE_REPLICATION_INTERVAL = "rivulet-state-replication-interval";
    private static final String ACTIVE_SCALING_PERIOD = "rivulet-scaling-period-in-mins";
    private static final String CHECKPOINT_TIMEOUT_PERIOD = "rivulet-checkpoint-timeout";

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
    private ComputationMonitor monitoringTask;
    private IMap<String, Double> memoryUsageMap;
    private String dataEndpoint;

    private boolean enableFaultTolerance = false;
    private long stateReplicationInterval = 2000;
    private long activeScalingPeriod;
    private long tsActiveScalingStarted;
    private long checkpointTimeoutPeriod;
    private ScheduledFuture future;
    private int seqNoStart;
    private int seqNoEnd;

    private Map<String, NOAADataIngester> registeredIngesters = new HashMap<>();

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
            dataEndpoint = RivuletUtil.getDataEndpoint();

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
                this.monitoringTask = new ComputationMonitor();
                future = monitoringService.scheduleWithFixedDelay(monitoringTask, 0, monitoringPeriod,
                        TimeUnit.MILLISECONDS);
                logger.info(String.format("Scale-in Threshold: %d, Scale-out Threshold: %d, " +
                                "Monitoring Period: %d (ms), Monitored Backlog History Length: %d", scaleInThreshold,
                        scaleOutThreshold, monitoringPeriod, monitoredBackLogLength));
            }
            activeScalingPeriod = startupProps.containsKey(ACTIVE_SCALING_PERIOD) ?
                    Integer.parseInt(startupProps.getProperty(ACTIVE_SCALING_PERIOD)) * 60 * 1000 : -1;

            // if fault tolerance enabled
            enableFaultTolerance = startupProps.containsKey(ENABLE_FAULT_TOLERANCE) && Boolean.parseBoolean(
                    startupProps.getProperty(ENABLE_FAULT_TOLERANCE).toLowerCase());
            if (enableFaultTolerance) {
                stateReplicationInterval = startupProps.containsKey(STATE_REPLICATION_INTERVAL) ?
                        Long.parseLong(startupProps.getProperty(STATE_REPLICATION_INTERVAL)) : 2000;
                checkpointTimeoutPeriod = startupProps.containsKey(CHECKPOINT_TIMEOUT_PERIOD) ?
                        Long.parseLong(startupProps.getProperty(CHECKPOINT_TIMEOUT_PERIOD)) : 6000;
            }

            logger.info(String.format("Fault tolerance enabled: %b, Active Scaling Period: %d", enableFaultTolerance, activeScalingPeriod));

            initializeHazelcast(startupProps);
            // register callback to receive deployment acks.
            ChannelToStreamDeMultiplexer.getInstance().registerCallback(Constants.DEPLOYMENT_REQ,
                    new DeployerCallback(this));
            countDownLatch.await();

            // initiate seq. no generation
            setUpSeqNoGenerator();
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
        RivuletUtil.initializeHazelcast(startupProps);
        try {
            IMap map = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance().getMap(
                    GeoHashPrefixTree.PREFIX_MAP);
            map.addEntryListener(GeoHashPrefixTree.getInstance(), true);
            memoryUsageMap = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance().getMap(
                    neptune.geospatial.graph.Constants.MEMORY_USAGE_MAP);
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
            if (!monitoredProcessors.containsKey(instanceIdentifier)) {
                monitoredProcessors.put(instanceIdentifier, new MonitoredComputationState(processor));
                logger.info("New processor is registered. Added to the monitoring list. Identifier: " + instanceIdentifier);
            }
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
        this.monitoringTask.eligibleForScaling.set(true);
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
                if (ctrlMessage instanceof CheckpointAck && registeredIngesters.containsKey(computationId)) {
                    registeredIngesters.get(computationId).handleControlMessage(ctrlMessage);
                } else {
                    logger.warn(String.format("Invalid control message to computation : %s, type: %d", computationId,
                            ctrlMessage.getMessageType()));
                }
            }
        }
    }

    public void dispatchPrefixOnlyScaleOutAck(PrefixOnlyScaleOutCompleteAck scaleOutComplete) {
        String ingesterId = scaleOutComplete.getIngesterId();
        if (registeredIngesters.containsKey(ingesterId)) {
            registeredIngesters.get(ingesterId).handlePrefixOnlyScaleOutAck(scaleOutComplete);
        } else {
            logger.warn("Invalid PrefixOnlyScaleOutCompleteAck. Ingester id: " + ingesterId);
        }
    }

    public void dispatchEnableShortCircuitingMessage(EnableShortCircuiting enableShortCircuiting) {
        String ingesterId = enableShortCircuiting.getTarget();
        if (registeredIngesters.containsKey(ingesterId)) {
            registeredIngesters.get(ingesterId).handleEnableShortCircuitMessage(enableShortCircuiting);
        } else {
            logger.warn("Invalid EnableShortCircuiting message. Ingester id: " + ingesterId);
        }
    }

    public synchronized void registerIngester(NOAADataIngester ingester) {
        String ingesterId = ingester.getInstanceIdentifier();
        if (!registeredIngesters.containsKey(ingesterId)) {
            registeredIngesters.put(ingesterId, ingester);
            logger.info("Successfully registered the stream ingester: " + ingesterId);
        } else {
            logger.warn("Ingester already registered. Ingester Id: " + ingesterId);
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

    public long getCheckpointTimeoutPeriod() {
        return checkpointTimeoutPeriod;
    }

    public long getStateReplicationInterval() {
        return stateReplicationInterval;
    }

    private synchronized void setUpSeqNoGenerator() {
        InetAddress inetAddress = RivuletUtil.getHostInetAddress();
        if (inetAddress.isLoopbackAddress()) {
            try {
                String port = RivuletUtil.getCtrlEndpoint().split(":")[1];
                seqNoStart = Integer.parseInt(port);
            } catch (GranulesConfigurationException e1) {
                // worst case, resort to a random number
                seqNoStart = new Random(System.currentTimeMillis()).nextInt();
            }
        } else {
            String[] ipAddr = RivuletUtil.getHostInetAddress().getHostAddress().trim().split("\\.");
            seqNoStart = Integer.parseInt(ipAddr[2] + ipAddr[3]);
        }
        seqNoStart = seqNoStart * 1000;
        seqNoEnd = seqNoStart + 1000;
        logger.info(String.format("Seq. No. Range for topic creation: [%d - %d)", seqNoStart, seqNoEnd));
    }

    public synchronized int getNextSeqNo() {
        return (++seqNoStart < seqNoEnd) ? seqNoStart : -1;
    }

    void handleQueryRequest(ClientQueryRequest clientQueryRequest) {
        List<String> prefixes = clientQueryRequest.getGeoHashes();
        Map<String, List<String>> targets = new HashMap<>();
        int totalComputationCount = 0;
        for (String prefix : prefixes) {
            Map<String, String> locations = GeoHashPrefixTree.getInstance().query(prefix);
            for (String compId : locations.keySet()) {
                String endpoint = locations.get(compId);
                if (targets.containsKey(endpoint)) {
                    targets.get(endpoint).add(compId);
                } else {
                    List<String> comps = new ArrayList<>();
                    comps.add(compId);
                    targets.put(endpoint, comps);
                }
                totalComputationCount++;
            }
        }
        for (String endpoint : targets.keySet()) {
            TargetedQueryRequest targetedQueryRequest = new TargetedQueryRequest(clientQueryRequest.getQueryId(),
                    clientQueryRequest.getQuery(), targets.get(endpoint), clientQueryRequest.getClientUrl());
            try {
                SendUtility.sendControlMessage(endpoint, targetedQueryRequest);
                logger.info("Sent a target query request to " + endpoint);
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending targeted query req. to " + endpoint, e);
            }
        }
        ClientQueryResponse clientQueryResponse = new ClientQueryResponse(clientQueryRequest.getQueryId(),
                totalComputationCount);
        logger.info("Sent back the query response back to client. Total number of target computations: " +
                totalComputationCount);
        try {
            SendUtility.sendControlMessage(clientQueryRequest.getClientUrl(), clientQueryResponse);
        } catch (CommunicationsException | IOException e) {
            logger.error("Error sending back the query response back to the client.", e);
        }
    }
}
