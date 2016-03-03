package neptune.geospatial.graph.operators;

import ds.granules.streaming.core.StreamSource;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Stream ingester for NOAA dataset.
 *
 * @author Thilina Buddhika
 */
public class NOAADataIngester extends StreamSource {

    private Logger logger = Logger.getLogger(NOAADataIngester.class);
    private static final int PRECISION = 5;

    private File[] inputFiles;
    private int indexLastReadFile = 0;
    private int countTotal = 0;
    protected int countEmitted = 0;
    private SerializationInputStream inStream;
    private long messageSeqId = 0;

    public NOAADataIngester() {
        String hostname = RivuletUtil.getHostInetAddress().getHostName();
        String dataDirPath = "/s/" + hostname + "/b/nobackup/galileo/noaa-dataset/bundles/";
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
        logger.info(String.format("[Stream Ingestor: %s] Number of input files: %d", getInstanceIdentifier(),
                inputFiles.length));
    }

    @Override
    public void emit() throws StreamingDatasetException {
        GeoHashIndexedRecord record = nextRecord();
        if (record != null) {
            writeToStream(Constants.Streams.NOAA_DATA_STREAM, record);
            countEmitted++;
        }
    }

    protected GeoHashIndexedRecord nextRecord() {
        if (inputFiles.length == 0) { // no input files, return
            return null;
        }
        if (indexLastReadFile == 0 && countTotal == 0) { // reading the very first record
            startNextFile();
            return parse();
        } else if (countEmitted < countTotal) { // in the middle of a file
            return parse();
        } else if (indexLastReadFile < inputFiles.length && countTotal == countEmitted) { // start next file.
            startNextFile();
            return parse();
        }
        return null;    // completed reading all files.
    }

    private void startNextFile() {
        try {
            FileInputStream fIn = new FileInputStream(inputFiles[indexLastReadFile++]);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            this.inStream = new SerializationInputStream(bIn);
            this.countTotal = inStream.readInt();
            countEmitted = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private GeoHashIndexedRecord parse() {
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


}
