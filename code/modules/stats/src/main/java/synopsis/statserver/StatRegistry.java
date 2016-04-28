package synopsis.statserver;

import neptune.geospatial.stat.InstanceRegistration;
import neptune.geospatial.stat.PeriodicInstanceMetrics;
import neptune.geospatial.stat.ScaleActivity;
import neptune.geospatial.stat.StatConstants;
import org.apache.log4j.Logger;
import synopsis.statserver.processors.ScaleActivityProcessor;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process statistics messages and maintains the live state of the entire system
 *
 * @author Thilina Buddhika
 */
class StatRegistry implements Runnable {

    private static final StatRegistry instance = new StatRegistry();
    private final Logger logger = Logger.getLogger(StatRegistry.class);

    private Map<String, double[]> processorRegistry = new HashMap<>();
    private Map<String, double[]> ingesterRegistry = new HashMap<>();
    private Map<MetricProcessor, BufferedWriter> processors = new HashMap<>();

    private AtomicBoolean shutdownFlag = new AtomicBoolean(false);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private ScheduledExecutorService periodMetricCalcService = Executors.newScheduledThreadPool(1);
    private Future periodicCalTaskFuture;
    private boolean zeroRegistrations = true;
    private List<ScalingActivity> completedScalingActivities = new ArrayList<>();
    private Map<String, ScalingActivity> onGoingActivities = new HashMap<>();
    private ScaleActivityProcessor scaleActivityProcessor;
    private BufferedWriter scaleActivityBuff;

    private StatRegistry() {
        scaleActivityProcessor = new ScaleActivityProcessor();
        try {
            scaleActivityBuff = new BufferedWriter(new FileWriter("/tmp/" + scaleActivityProcessor.getOutputFileName()
                    + ".stat"));
        } catch (IOException e) {
            logger.error("Error creating output buffer for scale activity monitor.", e);
        }
    }

    static StatRegistry getInstance() {
        return instance;
    }

    synchronized void register(InstanceRegistration registerMessage) {
        String instanceId = registerMessage.getInstanceId();
        String originEndpoint = registerMessage.getOriginEndpoint();
        if (registerMessage.isInstanceType() == StatConstants.ProcessorTypes.PROCESSOR) {
            if (!processorRegistry.containsKey(instanceId)) {
                processorRegistry.put(instanceId, new double[]{-1.0, -1.0, -1.0, -1.0});
                logger.info(String.format("Registered new processor instance. Instance Id: %s, Endpoint: %s " +
                        "Registered Processor count: %d", instanceId, originEndpoint, processorRegistry.size()));
            } else {
                logger.warn(String.format("Duplicate registration message. Instance id: %s, Endpoint: %s",
                        instanceId, originEndpoint));
            }
        } else {
            if (!ingesterRegistry.containsKey(instanceId)) {
                ingesterRegistry.put(instanceId, new double[]{-1.0, -1.0});
                logger.info(String.format("Registered new ingester instance. Ingester id:%s, Endpoint: %s, " +
                                "Registered Ingester count: %d", instanceId, originEndpoint,
                        ingesterRegistry.size()));
            } else {
                logger.warn(String.format("Duplicate registration message. Instance id: %s, Endpoint: %s",
                        instanceId, originEndpoint));
            }
        }
    }

    synchronized void updateMetrics(PeriodicInstanceMetrics msg) {
        String instanceId = msg.getInstanceId();
        if (msg.isInstanceType() == StatConstants.ProcessorTypes.PROCESSOR) {
            if (processorRegistry.containsKey(instanceId)) {
                processorRegistry.put(instanceId, msg.getMetrics());
            } else {
                logger.warn(String.format("Invalid processor metrics update. Instance Id: %s, Endpoint: %s", instanceId,
                        msg.getOriginEndpoint()));
            }
        } else {
            if (ingesterRegistry.containsKey(instanceId)) {
                ingesterRegistry.put(instanceId, msg.getMetrics());
            } else {
                logger.warn(String.format("Invalid ingester metrics update. Instance Id: %s, Endpoint: %s", instanceId,
                        msg.getOriginEndpoint()));
            }
        }
        if (zeroRegistrations) {
            zeroRegistrations = false;
            periodicCalTaskFuture = periodMetricCalcService.scheduleAtFixedRate(this, 0, 4000, TimeUnit.MILLISECONDS);
        }
    }

    synchronized void registerProcessor(MetricProcessor processor) throws StatServerException {
        String outputFileName = String.format("/tmp/%s.stat", processor.getOutputFileName());
        try {
            this.processors.put(processor, new BufferedWriter(new FileWriter(outputFileName)));
        } catch (FileNotFoundException e) {
            String errMsg = "Error openning output stream to write results from " + processor.getClass().getName();
            throw new StatServerException(errMsg, e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    synchronized void processScalingActivity(ScaleActivity msg) {
        long now = System.currentTimeMillis();
        String instanceId = msg.getInstanceId();
        if (msg.isEventType() == StatConstants.ScaleActivityEvent.START) {
            if(onGoingActivities.containsKey(instanceId)){
                logger.error(String.format("START event is already recorded for instance %s. Endpoint: %s",
                        instanceId, msg.getOriginEndpoint()));
                return;
            }
            ScalingActivity activityObj = new ScalingActivity(instanceId, msg.getOriginEndpoint(), msg.isActivityType(), now);
            onGoingActivities.put(instanceId, activityObj);
        } else {
            ScalingActivity activity = onGoingActivities.remove(instanceId);
            if (activity == null) {
                logger.error(String.format("END event for invalid scaling activity. Instance id: %s, Endpoint: %s",
                        instanceId, msg.getOriginEndpoint()));
                return;
            }
            activity.setEndTime(now);
            completedScalingActivities.add(activity);
        }
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                long ts = System.currentTimeMillis();
                for (MetricProcessor processor : processors.keySet()) {
                    try {
                        if (processor.isForIngesters()) {
                            processor.process(ingesterRegistry, ts, processors.get(processor));
                        } else {
                            processor.process(processorRegistry, ts, processors.get(processor));
                        }
                    } catch (IOException e) {
                        logger.error(String.format("Processor %s failed", processor.getClass().toString()));
                    }
                }
                scaleActivityProcessor.process(completedScalingActivities, scaleActivityBuff);
            }
            for (BufferedWriter buffW : processors.values()) {
                try {
                    buffW.write('\n');
                    buffW.flush();
                } catch (IOException e) {
                    logger.error("Error flushing the output stream.", e);
                }
            }
            scaleActivityBuff.flush();
            if (shutdownFlag.get()) {
                for (BufferedWriter buffW : processors.values()) {
                    try {
                        buffW.close();
                    } catch (IOException e) {
                        logger.error("Error closing the output stream.", e);
                    }
                }
                scaleActivityBuff.close();
                shutdownLatch.countDown();
            }

        } catch (Throwable e) {
            logger.error("Error during metric calculation.", e);
        }
    }

    void shutDown() {
        shutdownFlag.set(true);
        if (periodicCalTaskFuture != null) {
            periodicCalTaskFuture.cancel(false);
            try {
                shutdownLatch.await();
            } catch (InterruptedException ignore) {
                return;
            }
        }
        logger.info("Stat Registry is shutting down.");
    }
}
