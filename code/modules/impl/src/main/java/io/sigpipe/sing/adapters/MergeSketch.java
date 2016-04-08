package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.serialization.SerializationInputStream;

public class MergeSketch {

    public static void main(String[] args) throws Exception {

        Sketch s = new Sketch();

        FileInputStream fIn = new FileInputStream("9.bin");
        GZIPInputStream gIn = new GZIPInputStream(fIn);
        BufferedInputStream bIn = new BufferedInputStream(gIn);
        SerializationInputStream sIn = new SerializationInputStream(bIn);

        s.merge(s.getRoot(), sIn);
        System.out.println(s.getMetrics());
        System.out.println(s.getRoot().numLeaves());
        System.out.println(s.getRoot().numDescendants());

        sIn.close();

        fIn = new FileInputStream("d.bin");
        gIn = new GZIPInputStream(fIn);
        bIn = new BufferedInputStream(gIn);
        sIn = new SerializationInputStream(bIn);

        s.merge(s.getRoot(), sIn);
        System.out.println(s.getMetrics());
        System.out.println(s.getRoot().numLeaves());
        System.out.println(s.getRoot().numDescendants());

        sIn.close();
    }

}
