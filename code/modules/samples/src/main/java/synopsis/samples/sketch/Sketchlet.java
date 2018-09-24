package synopsis.samples.sketch;

import io.sigpipe.sing.serialization.SerializationInputStream;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.SketchProcessor;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.io.*;

public class Sketchlet extends SketchProcessor {
    private static final int GEO_HASH_PRECISION = 5;
    private final Logger logger = Logger.getLogger(Sketchlet.class);
    private long messageIdentifier = 0;

    public void ingest(String inputFilePath) {
        if (!inputFilePath.endsWith(".mblob")) {
            logger.error("Incorrect file type.");   // we work with binary input files with .mblob extension.
            return;
        }
        try {
            FileInputStream fIn = new FileInputStream(inputFilePath);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            SerializationInputStream inStream = new SerializationInputStream(bIn);
            int recordCount = inStream.readInt();   // read the number of records in the current file.
            logger.info("Total number of records: " + recordCount);
            for (int i = 0; i < recordCount; i++) {     // for each record
                float lat = inStream.readFloat();   // latitude
                float lon = inStream.readFloat(); // longitude
                byte[] payload = inStream.readField(); // observational data
                String stringHash = GeoHash.encode(lat, lon, GEO_HASH_PRECISION); // calculate the geohash from lat and lon
                GeoHashIndexedRecord record = new GeoHashIndexedRecord(stringHash,
                        2,  // we only consider the first 2 characters when inserting observations into the sketch
                        ++messageIdentifier,    // monotonically increasing ids - used by the underlying stream processing engine of Synopsis
                        System.currentTimeMillis(), // ingestion timestamp - used to track the ingestion latency
                        payload);   // actual observational records

                // update the sketch - take a look at the neptune.geospatial.graph.operators.SketchProcessor class
                super.process(record);
            }
            logger.info("Completed ingesting all records.");
        } catch (IOException e) {
            logger.error("Deserialization Error!.", e);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: Sketchlet <input>");
            return;
        }
        Sketchlet sketchlet = new Sketchlet();
        sketchlet.ingest(args[0]);
    }
}
