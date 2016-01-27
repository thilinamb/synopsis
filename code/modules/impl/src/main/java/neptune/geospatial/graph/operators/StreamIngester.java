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
    private int c = 0;

    public StreamIngester() {
        try {
            bfr = new BufferedReader(new FileReader("/Users/thilina/csu/research/dsg/data/noaa_nam_pts.txt"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void emit() throws StreamingDatasetException {
        GeoHashIndexedRecord record = getNextRecord();
        if (record != null){
            writeToStream(Constants.Streams.GEO_HASH_INDEXED_RECORDS, record);
        }
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        declareStream(Constants.Streams.GEO_HASH_INDEXED_RECORDS, GeoHashIndexedRecord.class.getName());
    }

    private GeoHashIndexedRecord getNextRecord(){
        String line;
        GeoHashIndexedRecord record =  null;
        try {
            if (bfr != null){
                if((line = bfr.readLine()) != null) {
                    record = parse(line);
                } else {
                    // continuously read the file, so that we have enough data
                    bfr =  new BufferedReader(new FileReader("/Users/thilina/csu/research/dsg/data/noaa_nam_pts.txt"));
                }
            }
        } catch (IOException e) {
            logger.error("Error reading from the file.", e);
        }
        return record;
    }

    private GeoHashIndexedRecord parse(String line){
        String[] locSegments = line.split("   ");
        GeoHashIndexedRecord record =  null;
        if(locSegments.length == 2){
            String geoHash = GeoHash.encode(Float.parseFloat(locSegments[0]), Float.parseFloat(locSegments[1]),
                    PRECISION);
            record = new GeoHashIndexedRecord(geoHash, System.currentTimeMillis());
        }
        return record;
    }
}
