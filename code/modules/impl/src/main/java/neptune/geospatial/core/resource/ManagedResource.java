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
import neptune.geospatial.core.protocol.AbstractProtocolHandler;
import neptune.geospatial.core.protocol.msg.TriggerScaleAck;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        private List<Long> backLogHistory = new ArrayList<>(MONITORED_BACKLOG_HISTORY_LENGTH);
        private AtomicBoolean eligibleForScaleOut = new AtomicBoolean(true);

        private MonitoredComputationState(GeoSpatialStreamProcessor computation) {
            this.computation = computation;
        }

        private boolean monitor() {
            long currentBacklog = computation.getBacklogLength();
            backLogHistory.add(currentBacklog);
            while (backLogHistory.size() > MONITORED_BACKLOG_HISTORY_LENGTH) {
                backLogHistory.remove(0);
            }
            return isBacklogDeveloping();
        }

        private boolean isBacklogDeveloping() {
            if (backLogHistory.size() >= MONITORED_BACKLOG_HISTORY_LENGTH) {
                boolean isDeveloping = true;
                for (int i = 0; i < backLogHistory.size(); i++) {
                    if (backLogHistory.get(i) < THRESHOLD) {
                        isDeveloping = false;
                        break;
                    }
                }
                return isDeveloping;
            } else {
                return false;
            }
        }
    }

    /**
     * Monitors the computations to detect the stragglers.
     */
    class ComputationMonitor implements Runnable {
        @Override
        public void run() {
            try {
                synchronized (ManagedResource.this) {
                    // wait till computations are registered. avoid busy waiting at the beginning.
                    if (monitoredProcessors.isEmpty()) {
                        monitoredProcessors.wait();
                    }
                    for (String identifier : monitoredProcessors.keySet()) {
                        MonitoredComputationState monitoredComputationState = monitoredProcessors.get(identifier);
                        if (monitoredComputationState.eligibleForScaleOut.get() && monitoredComputationState.monitor()) {
                            // trigger scale up
                            monitoredComputationState.eligibleForScaleOut.set(true);
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
    public static final long THRESHOLD = 5 * 1024;
    public static final int MONITORING_PERIOD = 10 * 1000;

    private static ManagedResource instance;
    private String deployerEndpoint = null;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private Map<String, MonitoredComputationState> monitoredProcessors = new HashMap<>();
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
        monitoringService.scheduleWithFixedDelay(new ComputationMonitor(), 0, MONITORING_PERIOD, TimeUnit.MILLISECONDS);
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

    public synchronized void registerStreamProcessor(GeoSpatialStreamProcessor processor) {
        monitoredProcessors.put(processor.getInstanceIdentifier(), new MonitoredComputationState(processor));
        monitoredProcessors.notifyAll();
    }

    public void scaleOutComplete(String computationIdentifier) {
        MonitoredComputationState monitoredComputationState = monitoredProcessors.get(computationIdentifier);
        monitoredComputationState.eligibleForScaleOut.set(true);
    }

    public void handleTriggerScaleAck(TriggerScaleAck ack) {
        String processorId = ack.getTargetComputation();
        if (monitoredProcessors.containsKey(processorId)) {
            monitoredProcessors.get(processorId).computation.handleTriggerScaleAck(ack);
        } else {
            logger.warn("ScaleTriggerAck for an invalid computation: " + processorId);
        }
    }
}
