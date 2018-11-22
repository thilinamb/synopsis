package synopsis.samples.sketch;

import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.SimplePair;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.graph.DataContainer;
import io.sigpipe.sing.query.*;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.util.ReducedTestConfiguration;
import neptune.geospatial.ft.FaultTolerantStreamBase;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.SketchProcessor;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Sketchlet extends SketchProcessor {
    private static final int GEO_HASH_PRECISION = 5;

    private static Map<String, SimplePair<Double>> ranges = new HashMap<>();
    static {
        /* Set up ranges for all the features */
        for (String feature : ReducedTestConfiguration.FEATURE_NAMES) {
            Quantizer quant = ReducedTestConfiguration.quantizers.get(feature);
            double first = quant.first().getDouble();
            double last = quant.last().getDouble();
            SimplePair<Double> range = new SimplePair<>();
            range.a = first;
            range.b = last;
            ranges.put(feature, range);
        }
    }
    private static final Logger LOGGER = Logger.getLogger(Sketchlet.class);
    private long messageIdentifier = 0;

    public void ingest(String inputFilePath) {
        if (!inputFilePath.endsWith(".mblob")) {
            LOGGER.error("Incorrect file type.");   // we work with binary input files with .mblob extension.
            return;
        }
        try {
            FileInputStream fIn = new FileInputStream(inputFilePath);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            SerializationInputStream inStream = new SerializationInputStream(bIn);
            int recordCount = inStream.readInt();   // read the number of records in the current file.
            LOGGER.info("Total number of records: " + recordCount);
            System.out.println("Record count: " + recordCount);
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
            LOGGER.info("Completed ingesting all records.");
        } catch (IOException e) {
            LOGGER.error("Deserialization Error!.", e);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: Sketchlet <input>");
            return;
        }
        Sketchlet sketchlet = new Sketchlet();
        sketchlet.ingest(args[0]);
        LOGGER.info("Ingestion is complete!");
        tryMetadataQuery(sketchlet);
        tryTemporalMetadataQuery(sketchlet);
        //tryRelationalQuery(sketchlet);
    }

    private static void tryMetadataQuery(Sketchlet sketchlet) {
        MetaQuery mq = new MetaQuery();
        mq.addExpression(new Expression(
                Operator.STR_PREFIX, new Feature("location", "9x")));
        // define a predicate for a feature - let's pick temperature
        // check the ranges hashmap defined above for the list of the featuers and their ranges
        /*mq.addExpression(
                new Expression(
                        Operator.RANGE_INC_EXC, // this is a range predicate
                        new Feature("temperature_surface", 230.0f), // temp. >= 280K
                        new Feature("temperature_surface", 300.0f))); // temp < 290K
        */
        try {
            mq.execute(sketchlet.sketch.getRoot());
            // results of the meta query is contained in a data container.
            //  please take a look at the returned object by attaching a debug pointer
            DataContainer dataContainer = mq.result();
            System.out.println("MetaQuery is executed.  Count: " + dataContainer.statistics.count());
        } catch (IOException | QueryException e) {
            e.printStackTrace();
        }
    }

    private static void tryTemporalMetadataQuery(Sketchlet sketchlet) {
        MetaQuery mq = new MetaQuery();
        mq.addExpression(new Expression(
                Operator.STR_PREFIX, new Feature("location", "9x")));
        mq.addExpression(   // temporal predicate
                new Expression(
                        Operator.GREATER,
                        new Feature("time", 1392206400000L))); // all observations occurred after 2014-02-12T12:00 UTC
        try {
            mq.execute(sketchlet.sketch.getRoot());
            // results of the meta query is contained in a data container.
            //  please take a look at the returned object by attaching a debug pointer
            DataContainer dataContainer = mq.result();
            System.out.println("Temporal MetaQuery is executed.  Observation Count: " + dataContainer.statistics.count());
        } catch (IOException | QueryException e) {
            e.printStackTrace();
        }
    }

    private static void tryRelationalQuery(Sketchlet sketchlet) {
        // executing relational query
        RelationalQuery rq = new RelationalQuery();
        // define the spatial scope of the query
        rq.addExpression(new Expression(
                Operator.STR_PREFIX, new Feature("location", "9x")));
        // define a predicate for a feature - let's pick temperature
        rq.addExpression(
                new Expression(
                        Operator.RANGE_INC_EXC, // this is a range predicate
                        new Feature("temperature_surface", 230.0f), // temp. >= 280K
                        new Feature("temperature_surface", 300.0f))); // temp < 290K

        try {
            rq.execute(sketchlet.sketch.getRoot()); // execute the query on the sketchlet
            if (rq.hasResults()) {
                // serialize the results into a byte array - the output of the relational query is again a sketch
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                SerializationOutputStream sOut = new SerializationOutputStream(
                        new BufferedOutputStream(byteOut));
                rq.serializeResults(sketchlet.sketch.getRoot(), sOut);
                sOut.close();
                byteOut.close();
                byte[] results = byteOut.toByteArray();
                LOGGER.info("Results available for the query: " + rq.toString());
            } else {
                LOGGER.info("Query did not match any data. Query: " + rq.toString());
            }

        } catch (IOException | QueryException e) {
            e.printStackTrace();
        }
    }
}
