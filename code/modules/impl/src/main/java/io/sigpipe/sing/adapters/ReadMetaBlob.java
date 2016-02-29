
package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Scanner;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.graph.DataContainer;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.stat.FeatureSurvey;
import io.sigpipe.sing.util.PerformanceTimer;

public class ReadMetaBlob {

    public static void main(String[] args) throws Exception {
        File bundle = new File(args[0]);

        System.out.println("Reading metadata blob...");
        FileInputStream fIn = new FileInputStream(bundle);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        SerializationInputStream in = new SerializationInputStream(bIn);

        int num = in.readInt();
        System.out.println("Records: " + num);

        Sketch s = new Sketch();

        FeatureSurvey fs = new FeatureSurvey();
        for (int i = 0; i < num; ++i) {
            float lat = in.readFloat();
            float lon = in.readFloat();
            byte[] payload = in.readField();

            Metadata m = Serializer.deserialize(Metadata.class, payload);

            for (Feature feat : m.getAttributes()) {
                fs.add(feat);
            }

            Path p = new Path(m.getAttributes().toArray());
            //PerformanceTimer pt = new PerformanceTimer("go");
            //pt.start();
            s.addPath(p);
            //pt.stopAndPrint();
            if (i % 1000 == 0) {
                System.out.print('.');
            }
        }
        System.gc();

        Scanner scan=new Scanner(System.in);
        scan.nextInt();

        System.out.println(s.getRoot().numLeaves());
        System.out.println(s.getRoot().numDescendants());
        System.out.println(s.getRoot().numDescendantEdges());

        scan.nextInt();

        //fs.printAll();
        PerformanceTimer serpt = new PerformanceTimer("serialize");
        serpt.start();
        Serializer.persistCompressed(s.getRoot(), "testvertex.bin");
        serpt.stopAndPrint();

        in.close();
    }
}
