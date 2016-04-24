package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.CountContainer;
import io.sigpipe.sing.graph.FeatureHierarchy;
import io.sigpipe.sing.graph.FeatureTypeMismatchException;
import io.sigpipe.sing.graph.GraphException;
import io.sigpipe.sing.graph.GraphMetrics;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.query.Expression;
import io.sigpipe.sing.query.Operator;
import io.sigpipe.sing.query.RelationalQuery;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.util.Geohash;
import io.sigpipe.sing.util.TestConfiguration;


public class SketchProcessor {

    private Sketch sketch;
    private Sketch diff;
    private FeatureHierarchy hierarchy;

    public SketchProcessor() {
        /* Populate the feature hierarchy */
        try {
            hierarchy = new FeatureHierarchy();
            for (String featureName : TestConfiguration.FEATURE_NAMES) {
                hierarchy.addFeature(featureName, FeatureType.FLOAT);
            }
            hierarchy.addFeature("location", FeatureType.STRING);
            this.sketch = new Sketch(hierarchy);
            this.diff = new Sketch(hierarchy);
        } catch (GraphException e) {
            System.out.println("Could not initialize sketch graph hierarchy");
            e.printStackTrace();
        }
    }

//    protected void process(GeoHashIndexedRecord event) {
//        Metadata eventMetadata = null;
//        try {
//            byte[] payload = event.getPayload();
//            eventMetadata = Serializer.deserialize(Metadata.class, payload);
//        } catch (Exception e) {
//            //TODO log this
//            System.out.println("Could not deserialize event payload");
//            e.printStackTrace();
//        }
//
//        try {
//            Path path = new Path(eventMetadata.getAttributes().toArray());
//            String shortLocation = event.getGeoHash().substring(0, 4);
//            path.add(new Feature("location", shortLocation));
//            this.sketch.addPath(path);
//            this.diff.addPath(path);
//        } catch (Exception e) {
//            System.out.println("Failed to insert graph path");
//            e.printStackTrace();
//        }
//    }

    public byte[] split(String prefix) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        try {
            SerializationOutputStream out
                = new SerializationOutputStream(new GZIPOutputStream(byteOut));

            RelationalQuery rq = new RelationalQuery(this.sketch.getMetrics());
            rq.addExpression(
                    new Expression(
                        Operator.STR_PREFIX, new Feature("location", prefix)));
            rq.execute(sketch.getRoot());
            rq.serializeResults(sketch.getRoot(), out);

            out.close();
        } catch (Exception e) {
            System.out.println("Failed to split sketch");
            e.printStackTrace();
        }

        return byteOut.toByteArray();
    }

    public void merge(String prefix, byte[] serializedSketch) {
        try {
            SerializationInputStream in = new SerializationInputStream(
                    new BufferedInputStream(
                        new GZIPInputStream(
                            new ByteArrayInputStream(serializedSketch))));

            this.sketch.merge(this.sketch.getRoot(), in);
            in.close();
        } catch (Exception e) {
            System.out.println("Failed to merge sketch");
            e.printStackTrace();
        }
    }

    public double getMemoryConsumptionForPrefix(String prefix) {
        CountContainer cc = this.sketch.geoTrie.query(prefix);
        long vertices = cc.a;
        long leaves = cc.b;
        return estimateMemoryUsage(vertices, leaves);
    }

    public double getMemoryConsumptionForAllPrefixes() {
        GraphMetrics gm = this.sketch.getMetrics();
        return estimateMemoryUsage(gm.getVertexCount(), gm.getLeafCount());
    }

    private double estimateMemoryUsage(long vertices, long leaves) {
        int bytesPerVertex = 16;

        int numFeatures = sketch.getFeatureHierarchy().size();
        int bytesPerLeaf = 8 + (8 * numFeatures * 4)
            + (8 * ((numFeatures * (numFeatures - 1)) / 2));

        return (bytesPerVertex * vertices) + (bytesPerLeaf * leaves);
    }

    public byte[] getSketchDiff() {
        byte[] diffBytes = null;
        try {
            diffBytes = Serializer.serialize(diff.getRoot());
            diff = new Sketch(hierarchy);
        } catch (Exception e) {
            System.out.println("Could not produce sketch diff");
            e.printStackTrace();
        }
        return diffBytes;
    }

    /*
    public void populateSketch(String baseDirPath) {

    }
    */

    public static void main(String[] args) throws Exception {
        SketchProcessor sp = new SketchProcessor();

        File bundle = new File(args[0]);
        FileInputStream fIn = new FileInputStream(bundle);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        SerializationInputStream in = new SerializationInputStream(bIn);

        int num = in.readInt();
        System.out.println("Records: " + num);

        for (int i = 0; i < num; ++i) {
            float lat = in.readFloat();
            float lon = in.readFloat();
            byte[] payload = in.readField();

            Metadata m = Serializer.deserialize(Metadata.class, payload);
            Path p = new Path(m.getAttributes().toArray());
            String location = Geohash.encode(lat, lon, 4);
            p.add(new Feature("location", location));
            sp.sketch.addPath(p);
        }

        byte[] ser = sp.split("9x");
        System.out.println(ser.length);
        sp.merge(null, ser);
    }
}