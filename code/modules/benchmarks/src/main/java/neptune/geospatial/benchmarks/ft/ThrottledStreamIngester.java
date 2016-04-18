package neptune.geospatial.benchmarks.ft;

import com.hazelcast.core.IQueue;
import ds.funnel.data.format.FormatReader;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.benchmarks.util.SineCurveLoadProfiler;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.ft.BackupTopicInfo;
import neptune.geospatial.ft.FaultTolerantStreamBase;
import neptune.geospatial.ft.protocol.CheckpointAck;
import neptune.geospatial.ft.zk.MembershipChangeListener;
import neptune.geospatial.ft.zk.MembershipTracker;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.NOAADataIngester;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Thilina Buddhika
 */
public class ThrottledStreamIngester extends NOAADataIngester implements FaultTolerantStreamBase, MembershipChangeListener {

    private Logger logger = Logger.getLogger(ThrottledStreamIngester.class);

    private final SineCurveLoadProfiler loadProfiler;
    private AtomicLong counter = new AtomicLong(0);
    private long tsLastEmitted = -1;
    private BufferedWriter bufferedWriter;
    // fault tolerance related configurations
    private Map<String, List<BackupTopicInfo>> topicLocations = new HashMap<>();
    private AtomicLong checkpointCounter = new AtomicLong(0);
    private long checkpointingInterval;
    private long tsLastCheckpoint = -1;
    private String currentEndpoint;

    public ThrottledStreamIngester() {
        super();
        loadProfiler = new SineCurveLoadProfiler(5000);
        try {
            bufferedWriter = new BufferedWriter(new FileWriter("/tmp/throughput-profile.stat"));
        } catch (IOException e) {
            logger.error("Error opening stat file for writing.", e);
        }
    }

    @Override
    public void emit() throws StreamingDatasetException {
        if (tsLastEmitted == -1) {
            try {
                Thread.sleep(20 * 1000);
                logger.debug("Initial sleep period is over. Starting to emit messages.");
            } catch (InterruptedException ignore) {

            }
        }
        GeoHashIndexedRecord record = nextRecord();
        long now = System.currentTimeMillis();
        if (record != null) {
            synchronized (this) {
                try {
                    writeToStream(Constants.Streams.NOAA_DATA_STREAM, record);
                    // trigger the downstream check-pointing
                    if (tsLastCheckpoint == -1) {
                        tsLastCheckpoint = now;
                    } else if (now - tsLastCheckpoint >= checkpointingInterval) {
                        GeoHashIndexedRecord checkpointTrigger = new GeoHashIndexedRecord(checkpointCounter.incrementAndGet(),
                                getInstanceIdentifier(), currentEndpoint);
                        writeToStream(Constants.Streams.NOAA_DATA_STREAM, checkpointTrigger);
                        tsLastCheckpoint = now;
                        if (logger.isDebugEnabled()) {
                            logger.debug("Initiated next checkpoint. Checkpoint id: " + checkpointCounter.get());
                        }
                    }
                } catch (StreamingDatasetException e) {
                    logger.error("Faulty downstream. Waiting till switching to a secondary topic.", e);
                    try {
                        this.wait();
                    } catch (InterruptedException ignore) {

                    }
                    logger.debug("Resuming after swithcing to secondary topic.");
                }
            }
            countEmitted++;
            long sentCount = counter.incrementAndGet();

            if (tsLastEmitted == -1) {
                tsLastEmitted = now;
                // register a listener for scaling in and out
                registerListener();
            } else if (now - tsLastEmitted > 1000) {
                try {
                    bufferedWriter.write(now + "," + sentCount * 1000.0 / (now - tsLastEmitted) + "\n");
                    bufferedWriter.flush();
                    tsLastEmitted = now;
                    counter.set(0);
                } catch (IOException e) {
                    logger.error("Error writing stats.", e);
                }
            }
            try {
                Thread.sleep(loadProfiler.nextSleepInterval());
            } catch (InterruptedException ignore) {

            }
        }
    }

    private void registerListener() {
        try {
            IQueue<Integer> scalingMonitorQueue = HazelcastClientInstanceHolder.getInstance().
                    getHazelcastClientInstance().getQueue("scaling-monitor");
            scalingMonitorQueue.addItemListener(new DynamicScalingMonitor(DynamicScalingGraph.INITIAL_PROCESSOR_COUNT),
                    true);
        } catch (HazelcastException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void deserializeMemberVariables(FormatReader formatReader) {
        // instance registration happens here
        super.deserializeMemberVariables(formatReader);
        // Hack: since Granules does not reinitialize operators after deploying
        // we need a way to read the backup topics from the zk tree just after the deployment.
        // doing it lazily is expensive.
        try {
            if (ManagedResource.getInstance().isFaultToleranceEnabled()) {
                this.checkpointingInterval = ManagedResource.getInstance().getStateReplicationInterval();
                MembershipTracker.getInstance().registerListener(this);
                this.topicLocations = populateBackupTopicMap(getInstanceIdentifier(), metadataRegistry);
                this.currentEndpoint = RivuletUtil.getCtrlEndpoint();
            }
        } catch (NIException e) {
            logger.error("Error acquiring the Resource instance.", e);
        } catch (CommunicationsException e) {
            logger.error("Error registering membership listener.", e);
        } catch (GranulesConfigurationException e) {
            logger.error("Error retrieving the current endpoint.", e);
        }
    }

    @Override
    public void membershipChanged(List<String> lostMembers) {
        synchronized (this) {
            boolean updateTopicLocations = false;
            for (String lostMember : lostMembers) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Processing the lost node: %s", getInstanceIdentifier(), lostMember));
                }
                // check if a node that hosts an out-going topic has left the cluster
                if (topicLocations.containsKey(lostMember)) {
                    // get the list of all topics that was running on the lost node
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Current node is affected by the lost node. Lost node: %s, " +
                                        "Number of affected topics: %d", getInstanceIdentifier(), lostMember,
                                topicLocations.get(lostMember).size()));
                    }
                    switchToSecondary(topicLocations, lostMember, getInstanceIdentifier(), metadataRegistry);
                    updateTopicLocations = true;
                }
            }
            // it is required to repopulate the backup nodes list
            if (updateTopicLocations) {
                populateBackupTopicMap(this.getInstanceIdentifier(), metadataRegistry);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] BackupTopicMap is updated.", getInstanceIdentifier()));
                }
            }
            this.notifyAll();
        }
    }

    @Override
    public void handleControlMessage(ControlMessage ctrlMsg) {
        if(ctrlMsg instanceof CheckpointAck){
            CheckpointAck ack = (CheckpointAck)ctrlMsg;
            if(logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received acknowledgments from all child nodes. " +
                        "Clearup the upstream back for checkpoint: %d",getInstanceIdentifier(), ack.getCheckpointId()));
            }
        }
    }
}
