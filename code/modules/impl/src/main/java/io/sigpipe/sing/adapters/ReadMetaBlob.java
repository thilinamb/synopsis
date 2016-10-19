
package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.FeatureHierarchy;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.query.Expression;
import io.sigpipe.sing.query.Operator;
import io.sigpipe.sing.query.PartitionQuery;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.util.Geohash;
import io.sigpipe.sing.util.PerformanceTimer;
import io.sigpipe.sing.util.ReducedTestConfiguration;
import io.sigpipe.sing.util.TestConfiguration;

public class ReadMetaBlob {

    public static Set<String> activeFeatures = new HashSet<>();

    public static void init() {
        for (String featureName : TestConfiguration.FEATURE_NAMES) {
            activeFeatures.add(featureName);
        }
    }

    public static void main(String[] args) throws Exception {
        init();

        Scanner scan=new Scanner(System.in);
        scan.nextInt();

        FeatureHierarchy fh = new FeatureHierarchy();
        for (String featureName : ReducedTestConfiguration.FEATURE_NAMES) {
            System.out.println(
                    ReducedTestConfiguration.quantizers.get(featureName).numTicks()
                    + "   " + featureName);
            fh.addFeature(featureName, FeatureType.FLOAT);
        }
        fh.addFeature("location", FeatureType.STRING);
        Sketch s = new Sketch(fh);

        loadData(args[0], s);

        System.gc();
        System.gc();
        
        System.out.println();
        Runtime runtime = Runtime.getRuntime();
        System.out.println("max=" + runtime.maxMemory());
        System.out.println("total=" + runtime.totalMemory());
        System.out.println("free=" + runtime.freeMemory());
        System.out.println("used=" + (runtime.totalMemory() - runtime.freeMemory()));
        System.out.println("estimate=" + estimateMemoryUsage(
                    s,
                    s.getMetrics().getVertexCount(),
                    s.getMetrics().getLeafCount()));

        //scan.nextInt();

        PerformanceTimer info = new PerformanceTimer("info");
        info.start();
        System.out.println(s.getRoot().numLeaves());
        System.out.println(s.getRoot().numDescendants());
        System.out.println(s.getRoot().numDescendantEdges());
        info.stopAndPrint();
        System.out.println(s.getMetrics());

        SerializationOutputStream out = new SerializationOutputStream(
                new GZIPOutputStream(
                new FileOutputStream(new File("d.bin"))));
        //s.getRoot().find(new Feature("location", 319920l));
        PartitionQuery pq = new PartitionQuery(s.getMetrics());
//        rq.addExpression(
//                new Expression(
//                    Operator.RANGE_INC, new Feature("temperature_surface", 260.0f), new Feature(300.0f)));
        String removePrefix = "d";
        pq.addExpression(
                new Expression(
                    Operator.STR_PREFIX, new Feature("location", removePrefix)));

        PerformanceTimer exec = new PerformanceTimer("exec");
        exec.start();
        pq.execute(s.getRoot());
        pq.serializeResults(s.getRoot(), out);
        out.close();
        exec.stopAndPrint();

        s.geoTrie.remove(removePrefix);

        System.gc();
        System.gc();
        System.out.println();
        System.out.println("max=" + runtime.maxMemory());
        System.out.println("total=" + runtime.totalMemory());
        System.out.println("free=" + runtime.freeMemory());
        System.out.println("used=" + (runtime.totalMemory() - runtime.freeMemory()));
        System.out.println("estimate=" + estimateMemoryUsage(
                    s,
                    s.getMetrics().getVertexCount(),
                    s.getMetrics().getLeafCount()));

        info.start();
        System.out.println(s.getRoot().numLeaves());
        System.out.println(s.getRoot().numDescendants());
        System.out.println(s.getRoot().numDescendantEdges());
        info.stopAndPrint();

        //scan.nextInt();

        System.out.println(s.getMetrics());

        System.out.println(s.geoTrie.query("dj").b);

        loadData(args[1], s);
        System.gc();
        System.gc();
        System.out.println();
        System.out.println("max=" + runtime.maxMemory());
        System.out.println("total=" + runtime.totalMemory());
        System.out.println("free=" + runtime.freeMemory());
        System.out.println("used=" + (runtime.totalMemory() - runtime.freeMemory()));
        System.out.println("estimate=" + estimateMemoryUsage(
                    s,
                    s.getMetrics().getVertexCount(),
                    s.getMetrics().getLeafCount()));
    }

    public static void loadData(String fileName, Sketch s)
    throws Exception {
        System.out.println("Reading metadata blob: " + fileName);
        FileInputStream fIn = new FileInputStream(fileName);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        SerializationInputStream in = new SerializationInputStream(bIn);

        int num = in.readInt();
        System.out.println("Records: " + num);

        PerformanceTimer addAllPaths = new PerformanceTimer("addAllPaths");
        addAllPaths.start();
        for (int i = 0; i < num; ++i) {
            float lat = in.readFloat();
            float lon = in.readFloat();
            byte[] payload = in.readField();

            Metadata m = Serializer.deserialize(Metadata.class, payload);

            Path p = new Path(activeFeatures.size() + 1);
            for (Feature f : m.getAttributes()) {
                String featureName = f.getName();
                if (activeFeatures.contains(featureName) == false) {
                    continue;
                }

                Quantizer q = ReducedTestConfiguration.quantizers.get(featureName);
                if (q == null) {
                    continue;
                }

                Feature quantizedFeature = q.quantize(f);
                p.add(new Feature(f.getName().intern(), quantizedFeature));
            }

            String location = Geohash.encode(lat, lon, 4);
            p.add(new Feature("location", location));
            s.addPath(p);

            if (i % 5000 == 0) {
                System.out.print('.');
            }
        }
        System.out.println();
        addAllPaths.stopAndPrint();

        in.close();
    }

    private static double estimateMemoryUsage(
            Sketch sketch, long vertices, long leaves) {
        int bytesPerVertex = 16;

        int numFeatures = sketch.getFeatureHierarchy().size();
        int bytesPerLeaf = 8 + (8 * numFeatures * 4)
            + (8 * ((numFeatures * (numFeatures - 1)) / 2));

        return ((bytesPerVertex * vertices) + (bytesPerLeaf * leaves)) * 1.7;
    }
}
