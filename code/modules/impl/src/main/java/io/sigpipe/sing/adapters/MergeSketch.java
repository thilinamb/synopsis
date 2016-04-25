package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.FeatureHierarchy;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.util.Geohash;
import io.sigpipe.sing.util.PerformanceTimer;
import io.sigpipe.sing.util.TestConfiguration;

public class MergeSketch {

    public static void main(String[] args) throws Exception {
        SketchProcessor sp = new SketchProcessor();

        List<GeoHashIndexedRecord> records1 = createRecords(args[0]);
        List<GeoHashIndexedRecord> records2 = createRecords(args[1]);

        PerformanceTimer pt = new PerformanceTimer("time");

        System.out.println("Building sketch...");
        pt.start();
        for (GeoHashIndexedRecord record : records1) {
            sp.process(record);
        }
        pt.stopAndPrint();
        System.out.println(sp.getGraphMetrics());

        System.out.println("Splitting...");
        pt.start();
        byte[] region = sp.split("djk");
        pt.stopAndPrint();
        System.out.println(sp.getGraphMetrics());

        System.out.println("Building new sketch");
        SketchProcessor miniSp = new SketchProcessor();
        miniSp.merge(null, region);
        System.out.println(miniSp.getGraphMetrics());
        for (GeoHashIndexedRecord record : records2) {
            miniSp.process(record);
        }
        byte[] entireRegion = miniSp.split("");

        System.out.println("Merging...");
        pt.start();
        sp.merge(null, entireRegion);
        pt.stopAndPrint();
        System.out.println(sp.getGraphMetrics());

        byte[] big = sp.split("9");
        sp.merge(null, big);
        System.out.println(sp.getGraphMetrics());

    }

    public static List<GeoHashIndexedRecord> createRecords(String fileName)
    throws Exception {
        System.out.println("Reading metadata blob: " + fileName);
        FileInputStream fIn = new FileInputStream(fileName);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        SerializationInputStream in = new SerializationInputStream(bIn);

        int num = in.readInt();
        System.out.println("Records: " + num);

        List<GeoHashIndexedRecord> records = new ArrayList<>(num);
        for (int i = 0; i < num; ++i) {
            float lat = in.readFloat();
            float lon = in.readFloat();
            byte[] payload = in.readField();
            GeoHashIndexedRecord rec = new GeoHashIndexedRecord(
                    payload, Geohash.encode(lat, lon, 4));
            records.add(rec);

            if (i % 5000 == 0) {
                System.out.print('.');
            }
        }
        System.out.println();

        in.close();

        return records;
    }
}
