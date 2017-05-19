package neptune.geospatial.graph.operators;

import ds.funnel.data.format.FormatReader;
import ds.funnel.data.format.FormatWriter;
import ds.funnel.topic.Topic;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.dataset.DatasetException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.StreamSource;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import neptune.geospatial.core.protocol.msg.EnableShortCircuiting;
import neptune.geospatial.core.protocol.msg.scaleout.PrefixOnlyScaleOutCompleteAck;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.partitioner.ShortCircuitedRoutingRegistry;
import neptune.geospatial.stat.InstanceRegistration;
import neptune.geospatial.stat.PeriodicInstanceMetrics;
import neptune.geospatial.stat.StatClient;
import neptune.geospatial.stat.StatConstants;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stream ingester for NOAA dataset.
 *
 * @author Thilina Buddhika
 */
public class NOAADataIngester extends StreamSource {

    private class StatPublisher implements Runnable {

        private String instanceId = getInstanceIdentifier();
        private boolean firstAttempt = true;
        private StatClient statClient = StatClient.getInstance();

        @Override
        public void run() {
            if (firstAttempt) {
                InstanceRegistration instanceRegistration = new InstanceRegistration(instanceId,
                        StatConstants.ProcessorTypes.INGESTER);
                statClient.publish(instanceRegistration);
                firstAttempt = false;
            } else {
                double[] metrics = new double[]{totalEmittedMsgCount.doubleValue(), totalEmittedBytes.doubleValue()};
                PeriodicInstanceMetrics periodicInstanceMetrics = new PeriodicInstanceMetrics(instanceId,
                        StatConstants.ProcessorTypes.INGESTER, metrics);
                statClient.publish(periodicInstanceMetrics);
            }
        }
    }

    private Logger logger = Logger.getLogger(NOAADataIngester.class);
    public static final int PRECISION = 5;

    protected File[] inputFiles;
    protected int indexLastReadFile = 0;
    protected int totalMessagesInCurrentFile = 0;
    protected int countEmittedFromCurrentFile = 0;
    protected AtomicLong totalEmittedMsgCount = new AtomicLong(0);
    private AtomicLong totalEmittedBytes = new AtomicLong(0);
    private SerializationInputStream inStream;
    private long messageSeqId = 0;
    private ScheduledExecutorService statPublisherService = Executors.newScheduledThreadPool(1);
    private boolean doPrefixOnlyInit = false;
    private AtomicInteger completedRounds = new AtomicInteger(0);
    private String prefixFilePath;
    private BufferedReader prefixFileBuffReader;
    private AtomicInteger remainingPhase1ScaleOutAckCount = new AtomicInteger(0);
    private boolean readAllFiles = false;
    //private String[] years = new String[]{"2011", "2012", "2013", "2014", "2015"};
    protected String[] years = new String[]{"2011", "2012", "2013", "2014"};
    private int yearIndex = 0;
    protected int initialWaitPeriodMS = 10000;
    private Map<String, Long> prefixFreq = new HashMap<>();

    public NOAADataIngester() {
        init();
    }

    public NOAADataIngester(boolean doPrefixOnlyInit) {
        init();
        this.doPrefixOnlyInit = doPrefixOnlyInit;
    }

    private void init() {
        String dataDirPath = getRootDataDirPath();
        this.inputFiles = getInputFilesInDir(dataDirPath);
        prefixFilePath = "/s/chopin/a/grad/thilinab/research/data/noaa_nam_pts.txt";
        logger.info(String.format("[Stream Ingestor: %s] Number of input files: %d", getInstanceIdentifier(),
                inputFiles.length));
    }

    @Override
    public void emit() throws StreamingDatasetException {
        if (doPrefixOnlyInit && completedRounds.get() < 2 && remainingPhase1ScaleOutAckCount.get() == 0) {
            logger.info(String.format("[%s] Prefix Only initialization is set. Starting round :%d",
                    getInstanceIdentifier(), completedRounds.get()));
            GeoHashIndexedRecord prefRecord = getNextPrefixOnlyRecord();
            while (prefRecord != null) {
                writeToStream(Constants.Streams.NOAA_DATA_STREAM, prefRecord);
                prefRecord = getNextPrefixOnlyRecord();
            }
            logger.info(String.format("[%s] Completed emitting prefixes for round: %d",
                    getInstanceIdentifier(), completedRounds.get()));
            // trigger the scale out
            try {
                remainingPhase1ScaleOutAckCount.set(getLayer1ReceiverCount());
                writeToStream(Constants.Streams.NOAA_DATA_STREAM,
                        new GeoHashIndexedRecord(Constants.RecordHeaders.SCALE_OUT, getInstanceIdentifier(),
                                RivuletUtil.getCtrlEndpoint()));
                logger.info(String.format("[%s] Triggered prefix only scale out for round: %d", getInstanceIdentifier(),
                        completedRounds.get()));
            } catch (GranulesConfigurationException e) {
                logger.error("Error sending out the scale out at prefix init phase.", e);
            }
        } else {
            GeoHashIndexedRecord record = nextRecord();
            if (record != null) {
                try {
                    if (filter(record.getGeoHash(), record.getPayload())) {
                        writeToStream(Constants.Streams.NOAA_DATA_STREAM, record);
                        totalEmittedMsgCount.incrementAndGet();
                        totalEmittedBytes.addAndGet(record.getPayload().length);
                        updateFrequency(record.getGeoHash());
                        if (totalEmittedMsgCount.get() == 1) {
                            statPublisherService.scheduleAtFixedRate(new StatPublisher(), 0, 2, TimeUnit.SECONDS);
                        }
                        onSuccessfulEmission();
                    }
                } catch (StreamingDatasetException e) {
                    logger.error(e);
                }
                countEmittedFromCurrentFile++;
            }
        }
    }

    public void onSuccessfulEmission() {
    }

    protected GeoHashIndexedRecord nextRecord() {
        if (inputFiles.length == 0) { // no input files, return
            return null;
        }
        if (indexLastReadFile == 0 && totalMessagesInCurrentFile == 0) { // reading the very first record
            try {
                Thread.sleep(this.initialWaitPeriodMS);
            } catch (InterruptedException ignore) {

            }
            logger.info("Initial wait is complete. Starting the ingestion!");
            startNextFile();
            return parse();
        } else if (countEmittedFromCurrentFile < totalMessagesInCurrentFile) { // in the middle of a file
            return parse();
        } else if (indexLastReadFile < inputFiles.length && totalMessagesInCurrentFile == countEmittedFromCurrentFile) { // start next file.
            startNextFile();
            logger.info(String.format("Reading file: %d of %d [%s]", indexLastReadFile, inputFiles.length, years[yearIndex - 1]));
            return parse();
        } else if (indexLastReadFile == inputFiles.length) {
            if (startNextYear()) {
                startNextFile(); // if there is data for another year
                return parse();
            } else {    // finished reading all the data
                if (!readAllFiles) { // set a flag to stop flooding logs when it has completed processing data
                    readAllFiles = true;
                    logger.info("Completed reading all files.");
                    dumpFrequencyInfo();
                }
            }
        }
        return null;    // completed reading all files.
    }

    protected void startNextFile() {
        try {
            FileInputStream fIn = new FileInputStream(inputFiles[indexLastReadFile++]);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            this.inStream = new SerializationInputStream(bIn);
            this.totalMessagesInCurrentFile = inStream.readInt();
            countEmittedFromCurrentFile = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean startNextYear() {
        if (yearIndex >= years.length) {
            return false;
        } else {
            String dataDirPath = getRootDataDirPath();
            this.inputFiles = getInputFilesInDir(dataDirPath);
            this.indexLastReadFile = 0;
            logger.info("Starting to read data for the year: " + years[yearIndex - 1]);
            return true;
        }
    }

    private File[] getInputFilesInDir(String dataDirPath) {
        File dataDir = new File(dataDirPath);
        if (dataDir.exists()) {
            inputFiles = dataDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".mblob");
                }
            });
        } else {
            inputFiles = new File[0];
        }
        /*// use only a subset of files to test querying -----------
        if (inputFiles.length > 150) {
            File[] sample = new File[150];
            for(int i =0; i < 150; i++) {
                sample[i] = inputFiles[i];
            }
            inputFiles = sample;
        }
        // ---------------- */
        return inputFiles;
    }

    protected String getRootDataDirPath() {
        String hostname = RivuletUtil.getHostInetAddress().getHostName();
        return "/s/" + hostname + "/b/nobackup/galileo/noaa-data/" + years[yearIndex++] + "/";
    }

    protected GeoHashIndexedRecord parse() {
        GeoHashIndexedRecord record = null;
        try {
            float lat = inStream.readFloat();
            float lon = inStream.readFloat();
            byte[] payload = inStream.readField();
            String stringHash = GeoHash.encode(lat, lon, PRECISION);
            record = new GeoHashIndexedRecord(stringHash, 2, ++messageSeqId, System.currentTimeMillis(), payload);
        } catch (IOException e) {
            logger.error("Read error", e);
        }
        return record;
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        declareStream(Constants.Streams.NOAA_DATA_STREAM, GeoHashIndexedRecord.class.getName());
    }

    @Override
    protected void serializedMemberVariables(FormatWriter formatWriter) {
        formatWriter.writeBool(doPrefixOnlyInit);
    }

    @Override
    protected void deserializeMemberVariables(FormatReader formatReader) {
        this.doPrefixOnlyInit = formatReader.readBool();
        try {
            ManagedResource.getInstance().registerIngester(this);
        } catch (NIException e) {
            logger.error("Error registering the ingester.", e);
        }
    }

    public void handleControlMessage(ControlMessage ctrlMsg) {
    }

    private GeoHashIndexedRecord getNextPrefixOnlyRecord() {
        try {
            if (prefixFileBuffReader == null) {
                prefixFileBuffReader = new BufferedReader(new FileReader(prefixFilePath));
            }
            String line = prefixFileBuffReader.readLine();
            if (line != null) {
                return parse(line);
            }
        } catch (IOException e) {
            logger.error("Error reading the prefix file.", e);
        }
        return null;
    }

    private GeoHashIndexedRecord parse(String line) {
        String[] locSegments = line.split("   ");
        GeoHashIndexedRecord record = null;
        if (locSegments.length == 2) {
            String geoHash = GeoHash.encode(Float.parseFloat(locSegments[0]), Float.parseFloat(locSegments[1]),
                    PRECISION);
            record = new GeoHashIndexedRecord(Constants.RecordHeaders.PREFIX_ONLY, geoHash, 2);
        }
        return record;
    }

    private int getLayer1ReceiverCount() {
        List<StreamDisseminationMetadata> metaData = metadataRegistry.get(Constants.Streams.NOAA_DATA_STREAM);
        return metaData.get(0).topics.length;
    }

    public void handlePrefixOnlyScaleOutAck(PrefixOnlyScaleOutCompleteAck ack) {
        int remainingCount = remainingPhase1ScaleOutAckCount.decrementAndGet();
        if (remainingCount == 0) {
            completedRounds.incrementAndGet();
        }
        logger.info(String.format("Received a PrefixOnlyScaleOutCompleteAck from %s, " +
                "remaining count: %d, current round: %d", ack.getOriginEndpoint(), remainingCount, completedRounds.get()));
    }

    public void handleEnableShortCircuitMessage(EnableShortCircuiting enableShortCircuiting) {
        int topicId = Integer.parseInt(enableShortCircuiting.getTopic());
        // TODO: Need to keep track of this stream when performaing scaling-in, in order to flush the buffers
        String streamId = enableShortCircuiting.getFullStreamId();
        try {
            declareStream(streamId, enableShortCircuiting.getStreamType());
            // initialize the meta-data
            ShortCircuitedRoutingRegistry routingRegistry = ShortCircuitedRoutingRegistry.getInstance();
            Topic[] topics = deployStream(streamId, new int[]{topicId}, routingRegistry.getPartitioner());
            String[] prefixList = enableShortCircuiting.getPrefixList();
            for (String prefix : prefixList) {
                // add a rule
                routingRegistry.addShortCircuitedRoutingRule(prefix, topics[0]);
            }
            // flush buffers of existing topics
            flushBuffersImmediately(Constants.Streams.NOAA_DATA_STREAM);
            logger.info("Added new short circuiting rules. Count: " + prefixList.length);
        } catch (StreamingGraphConfigurationException | StreamingDatasetException e) {
            logger.error("Error processing EnableShortCircuiting message.", e);
        } catch (DatasetException e) {
            logger.error("Error when flushing buffers of existing outgoing topics.", e);
        }
    }

    private synchronized void updateFrequency(String geoHash) {
        String prefix = geoHash.substring(0, 4);
        long count = 0;
        if (prefixFreq.containsKey(prefix)) {
            count = prefixFreq.get(prefix);
        }
        prefixFreq.put(prefix, count + 1);
    }

    private synchronized void dumpFrequencyInfo() {
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            fileWriter = new FileWriter("/tmp/freq-info.fstat");
            bufferedWriter = new BufferedWriter(fileWriter);
            logger.info("Dumping frequency info. Number of prefixes: " + prefixFreq.size());
            for (String prefix : prefixFreq.keySet()) {
                bufferedWriter.write(prefix + "," + prefixFreq.get(prefix) + "\n");
            }
            bufferedWriter.flush();
            fileWriter.flush();
            bufferedWriter.close();
            logger.info("Successfully dumped freq. info to file.");
        } catch (IOException e) {
            logger.error("Error writing to file.", e);
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                logger.error("Error closing file stream.", e);
            }
        }
    }

    protected boolean filter(String geohash, byte[] payload) {
        return true;
    }
}

