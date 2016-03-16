package neptune.geospatial.benchmarks.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates a sequence of sleep intervals based on a sine curve
 *
 * @author Thilina Buddhika
 */
public class SineCurveLoadProfiler {

    private int timeSlice;  // How many milliseconds correspond to one degree
    private long startTimestamp;
    private AtomicLong cycles;

    public SineCurveLoadProfiler(int timeSlice) {
        this.timeSlice = timeSlice;
        this.startTimestamp = System.currentTimeMillis();
        this.cycles = new AtomicLong(0);
    }

    public long nextSleepInterval() {
        long timeFromStart = System.currentTimeMillis() - startTimestamp;
        cycles.set((timeFromStart / timeSlice) / 360);
        double angleInRadians = (timeFromStart / timeSlice % 360) * Math.PI / 180;
        return Math.round(3 + (9 * 0.5 * (1 - Math.sin(angleInRadians))));
    }

    public static void main(String[] args) {
        try {
            long count = 0;
            long writes = 0;
            long lastTs = System.currentTimeMillis();
            BufferedWriter buffW = new BufferedWriter(new FileWriter("/tmp/slice-2s.csv"));
            SineCurveLoadProfiler loadProfiler = new SineCurveLoadProfiler(2000);
            BigInteger bigI1 = new BigInteger(512, new Random(1));
            BigInteger bigI2 = new BigInteger(512, new Random(2));
            while (true) {
                // work
                bigI1.add(bigI2);
                bigI1.add(bigI2);
                count++;
                long now = System.currentTimeMillis();
                if(now - lastTs > 2000){
                    buffW.write(now + "," + count + "\n");
                    count = 0;
                    lastTs = now;
                    writes++;
                }
                if(writes % 10 == 0){
                    buffW.flush();
                }
                Thread.sleep(loadProfiler.nextSleepInterval());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
