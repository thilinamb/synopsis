package neptune.geospatial.core.resource;

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
import neptune.geospatial.core.computations.GeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.ScalingException;
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.msg.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
        private GeoSpatialStreamProcessor computation;
        private AtomicReference<ArrayList<Long>> backLogHistory = new AtomicReference<>(
                new ArrayList<Long>(MONITORED_BACKLOG_HISTORY_LENGTH));
        private AtomicBoolean eligibleForScaling = new AtomicBoolean(true);

        private MonitoredComputationState(GeoSpatialStreamProcessor computation) {
            this.computation = computation;
        }

        private double monitor() {
            long currentBacklog = computation.getBacklogLength();
            backLogHistory.get().add(currentBacklog);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Monitoring computation: %s. Adding a backlog: %d",
                        computation.getInstanceIdentifier(), currentBacklog));
            }
            while (backLogHistory.get().size() > MONITORED_BACKLOG_HISTORY_LENGTH) {
                backLogHistory.get().remove(0);
            }
            return isBacklogDeveloping();
        }

        private double isBacklogDeveloping() {
            double excess = 0;
            if (backLogHistory.get().size() >= MONITORED_BACKLOG_HISTORY_LENGTH) {
                boolean increasing = true;
                boolean decreasing = true;
                for (int i = 0; i < backLogHistory.get().size(); i++) {
                    double entry = backLogHistory.get().get(i);
                    if (entry < HIGH_THRESHOLD) {
                        increasing = false;
                    } else if (entry > LOW_THRESHOLD) {
                        decreasing = false;
                    }
                }
                if (increasing) {
                    excess = backLogHistory.get().get(backLogHistory.get().size() - 1) - HIGH_THRESHOLD;
                } else if (decreasing) {
                    excess = backLogHistory.get().get(backLogHistory.get().size() - 1) - LOW_THRESHOLD;
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
                                try {
                                    boolean success = monitoredComputationState.computation.recommendScaling(excess);
                                    monitoredComputationState.eligibleForScaling.set(!success);
                                } catch (ScalingException e) {
                                    logger.error("Error scaling the computation " +
                                            monitoredComputationState.computation.getInstanceIdentifier());
                                    monitoredComputationState.eligibleForScaling.set(true);
                                }
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
    public static final int MONITORED_BACKLOG_HISTORY_LENGTH = 5;
    public static final long HIGH_THRESHOLD = 100;
    public static final long LOW_THRESHOLD = 10;
    public static final int MONITORING_PERIOD = 5 * 1000;

    private static ManagedResource instance;
    private String deployerEndpoint = null;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private final Map<String, MonitoredComputationState> monitoredProcessors = new HashMap<>();
    private ScheduledExecutorService monitoringService = Executors.newSingleThreadScheduledExecutor();

    public ManagedResource(Properties inProps, int numOfThreads) throws CommunicationsException {
        Resource resource = new Resource(inProps, numOfThreads);
        resource.init();
        logger.info("Successfully Started ManagedResource.");
    }

    private void init() {
        // register the callbacks to receive interference related control messages.
        instance = this;
        AbstractProtocolHandler protoHandler = new ResourceProtocolHandler(this);
        ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, protoHandler);
        new Thread(protoHandler).start();
        // start the computation monitor thread
        //monitoringService.scheduleWithFixedDelay(new ComputationMonitor(), 0, MONITORING_PERIOD, TimeUnit.MILLISECONDS);
        try {
            deployerEndpoint = NeptuneRuntime.getInstance().getProperties().getProperty(
                    Constants.DEPLOYER_ENDPOINT);
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

    public void registerStreamProcessor(GeoSpatialStreamProcessor processor) {
        synchronized (monitoredProcessors) {
            monitoredProcessors.put(processor.getInstanceIdentifier(), new MonitoredComputationState(processor));
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

    public void handleScaleInLockReq(ScaleInLockRequest lockReq) {
        String computationId = lockReq.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInLockReq(lockReq);
        } else {
            logger.warn("Invalid ScaleInLockReq for computation: " + computationId);
        }
    }

    public void handleScaleInLockResp(ScaleInLockResponse lockResponse){
        String computationId = lockResponse.getComputation();
        if(monitoredProcessors.containsKey(computationId)){
            monitoredProcessors.get(computationId).computation.handleScaleInLockResponse(lockResponse);
        } else {
            logger.warn("Invalid ScaleInLockResponse for computation: " + computationId);
        }
    }

    public void handleScaleInActivateReq(ScaleInActivateReq activateReq){
        String computationId = activateReq.getTargetComputation();
        if (monitoredProcessors.containsKey(computationId)) {
            monitoredProcessors.get(computationId).computation.handleScaleInActivateReq(activateReq);
        } else {
            logger.warn("Invalid ScaleInLockReq for computation: " + computationId);
        }
    }

    public void handleStateTransferMsg(StateTransferMsg stateTransferMsg){
        String computationId = stateTransferMsg.getTargetComputation();
        if(monitoredProcessors.containsKey(computationId)){
            monitoredProcessors.get(computationId).computation.handleStateTransferReq(stateTransferMsg);
        } else {
            logger.warn("Invalid StateTransferMsg to " + computationId);
        }
    }
}
