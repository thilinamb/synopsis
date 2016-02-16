package neptune.geospatial.graph.operators;

import ds.granules.streaming.core.StreamSource;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;

import io.sigpipe.sing.serialization.SerializationInputStream;

import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.SpatiallyIndexedRecord;
import neptune.geospatial.util.geohash.GeoHash;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Geo-spatial streams are ingested into the system using
 * this operator.
 *
 * @author Thilina Buddhika
 */
public class GeoStreamIngester extends StreamSource {

    private Logger logger = Logger.getLogger(StreamIngester.class);

    private static final int PRECISION = 5;

    private int numRecords;
    private int currentRecord;
    private SerializationInputStream inStream;

    public GeoStreamIngester()
    throws FileNotFoundException, IOException {
        File blob = new File("namanl_218_20150224_1800_006.grb.mblob");
        FileInputStream fIn = new FileInputStream(blob);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        this.inStream = new SerializationInputStream(bIn);

        this.numRecords = inStream.readInt();
    }

    @Override
    public void emit() throws StreamingDatasetException {
        SpatiallyIndexedRecord record = getNextRecord();
        if (record != null) {
            writeToStream(Constants.Streams.SPATIAL_INDEXED_RECORDS, record);
        }
    }

    @Override
    protected void declareOutputStreams()
    throws StreamingGraphConfigurationException {
        declareStream(Constants.Streams.SPATIAL_INDEXED_RECORDS,
                SpatiallyIndexedRecord.class.getName());
    }

    private SpatiallyIndexedRecord getNextRecord() {
        if (currentRecord >= numRecords) {
            /* TODO: move on to the next file? */
            return null;
        }

        currentRecord++;

        SpatiallyIndexedRecord record = null;
        try {

            float lat = inStream.readFloat();
            float lon = inStream.readFloat();
            byte[] payload = inStream.readField();

            String stringHash = GeoHash.encode(lat, lon, PRECISION);
            long geohashBits = GeoHash.hashToLong(stringHash);

            record = new SpatiallyIndexedRecord(geohashBits, payload);

        } catch (IOException e) {
            logger.error("Read error", e);
        }

        return record;
    }
}
