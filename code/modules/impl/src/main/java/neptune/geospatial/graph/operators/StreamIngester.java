package neptune.geospatial.graph.operators;

import ds.granules.streaming.core.StreamSource;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Geo-spatial streams are ingested into the system using
 * this operator.
 *
 * @author Thilina Buddhika
 */
public class StreamIngester extends StreamSource {

    private static final int PRECISION = 9;

    private Logger logger = Logger.getLogger(StreamIngester.class);
    private BufferedReader bfr; // for the moment, let's assume we read streams from a file
    private final AtomicLong seqGenerator = new AtomicLong(0);
    private byte[] dummyPayload;

    public StreamIngester() {
        try {
            bfr = new BufferedReader(new FileReader("/Users/thilina/csu/research/dsg/data/noaa_nam_pts.txt"));
            dummyPayload = new byte[512];
            new Random().nextBytes(dummyPayload);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void emit() throws StreamingDatasetException {
        GeoHashIndexedRecord record = getNextRecord();
        if (record != null) {
            writeToStream(Constants.Streams.GEO_HASH_INDEXED_RECORDS, record);
        }
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        declareStream(Constants.Streams.GEO_HASH_INDEXED_RECORDS, GeoHashIndexedRecord.class.getName());
    }

    private GeoHashIndexedRecord getNextRecord() {
        String line;
        GeoHashIndexedRecord record = null;
        try {
            if (bfr != null) {
                if ((line = bfr.readLine()) != null) {
                    record = parse(line);
                } /*else {
                    // continuously read the file, so that we have enough data
                    bfr = new BufferedReader(new FileReader("/Users/thilina/csu/research/dsg/data/noaa_nam_pts.txt"));
                }*/
            }
        } catch (IOException e) {
            logger.error("Error reading from the file.", e);
        }
        return record;
    }

    private GeoHashIndexedRecord parse(String line) {
        String[] locSegments = line.split("   ");
        GeoHashIndexedRecord record = null;
        if (locSegments.length == 2) {
            String geoHash = GeoHash.encode(Float.parseFloat(locSegments[0]), Float.parseFloat(locSegments[1]),
                    PRECISION);
            record = new GeoHashIndexedRecord(geoHash, 2, seqGenerator.incrementAndGet(), System.currentTimeMillis(),
                    dummyPayload);
        }
        return record;
    }

    public static void main(String[] args) {
        StreamIngester ingester = new StreamIngester();
        /*Map<String, Integer> dist = new HashMap<>();
        GeoHashIndexedRecord record = ingester.getNextRecord();
        while (record != null) {
            String pref = record.getGeoHash().substring(0, 1);
            if (!dist.containsKey(pref)) {
                dist.put(pref, 1);
            } else {
                dist.put(pref, dist.get(pref) + 1);
            }
            record = ingester.getNextRecord();
        }
        for(String pref : dist.keySet()){
            System.out.println(String.format("Prefix: %s, Count: %d", pref, dist.get(pref)));
        }*/
        Set<String> uniquePrefixes = new HashSet<>();
        GeoHashIndexedRecord record = ingester.getNextRecord();
        int totalCount = 0;
        while(record != null){
            String pref = record.getGeoHash().substring(0, 4);
            uniquePrefixes.add(pref);
            totalCount++;
            if(totalCount % 100000 == 0){
                System.out.println("Total: " + totalCount);
            }
            record = ingester.getNextRecord();
        }
        System.out.println("Total prefix count: " + uniquePrefixes.size());
    }
}
