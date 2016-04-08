
package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Scanner;
import java.util.zip.GZIPOutputStream;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.FeatureHierarchy;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.graph.Vertex;
import io.sigpipe.sing.query.Expression;
import io.sigpipe.sing.query.Operator;
import io.sigpipe.sing.query.RelationalQuery;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.stat.FeatureSurvey;
import io.sigpipe.sing.util.Geohash;
import io.sigpipe.sing.util.PerformanceTimer;
import io.sigpipe.sing.util.TestConfiguration;

public class ReadMetaBlob {

    public static void main(String[] args) throws Exception {
        File bundle = new File(args[0]);

        System.out.println("Reading metadata blob...");
        FileInputStream fIn = new FileInputStream(bundle);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        SerializationInputStream in = new SerializationInputStream(bIn);

        int num = in.readInt();
        System.out.println("Records: " + num);

        FeatureHierarchy fh = new FeatureHierarchy();
        for (String featureName : TestConfiguration.FEATURE_NAMES) {
            System.out.println(
                    TestConfiguration.quantizers.get(featureName).numTicks()
                    + "   " + featureName);
            fh.addFeature(featureName, FeatureType.FLOAT);
        }
        fh.addFeature("location", FeatureType.STRING);
        Sketch s = new Sketch(fh);

        FeatureSurvey fs = new FeatureSurvey();

        for (int i = 0; i < num; ++i) {
            float lat = in.readFloat();
            float lon = in.readFloat();
            byte[] payload = in.readField();

            Metadata m = Serializer.deserialize(Metadata.class, payload);

            Path p = new Path(m.getAttributes().toArray());
            String location = Geohash.encode(lat, lon, 4);
            p.add(new Feature("location", location));
            s.addPath(p);

            for (Vertex v : p) {
                if (v.getLabel().getType() != FeatureType.STRING) {
                    fs.add(v.getLabel());
                }
            }

            if (i % 1000 == 0) {
                System.out.print('.');
            }
        }
        System.out.println();
//        System.gc();
//        System.gc();
//        
//        Runtime runtime = Runtime.getRuntime();
//        System.out.println("max=" + runtime.maxMemory());
//        System.out.println("total=" + runtime.totalMemory());
//        System.out.println("free=" + runtime.freeMemory());
//        System.out.println("used=" + (runtime.totalMemory() - runtime.freeMemory()));

        Scanner scan=new Scanner(System.in);
        scan.nextInt();

        PerformanceTimer info = new PerformanceTimer("info");
        info.start();
        System.out.println(s.getRoot().numLeaves());
        System.out.println(s.getRoot().numDescendants());
        System.out.println(s.getRoot().numDescendantEdges());
        info.stopAndPrint();

        scan.nextInt();
        in.close();

        SerializationOutputStream out = new SerializationOutputStream(
                new GZIPOutputStream(
                new FileOutputStream(new File("d.bin"))));
        //s.getRoot().find(new Feature("location", 319920l));
        RelationalQuery rq = new RelationalQuery(s.getMetrics());
//        rq.addExpression(
//                new Expression(
//                    Operator.RANGE_INC, new Feature("temperature_surface", 260.0f), new Feature(300.0f)));
        rq.addExpression(
                new Expression(
                    Operator.STR_PREFIX, new Feature("location", "d")));

        PerformanceTimer exec = new PerformanceTimer("exec");
        exec.start();
        rq.execute(s.getRoot());
        rq.serializeResults(s.getRoot(), out);
        exec.stopAndPrint();

//        System.out.println(mq.result().statistics.mean(0));
//        System.out.println(mq.result().statistics.mean(1));
//        System.out.println(mq.result().statistics.mean(2));
//        System.out.println(mq.result().statistics.mean(3));
//        System.out.println(mq.result().statistics.mean(4));
//
//        System.out.println(mq.result().statistics.max(0));
//        System.out.println(mq.result().statistics.max(1));
//        System.out.println(mq.result().statistics.max(2));
//        System.out.println(mq.result().statistics.max(4));
//
//        System.out.println(mq.result().statistics.count());
//
//        fs.printAll();
        out.close();
        System.out.println(s.getMetrics());
    }
}
