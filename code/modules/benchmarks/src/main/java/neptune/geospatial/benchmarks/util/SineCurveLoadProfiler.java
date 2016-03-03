package neptune.geospatial.benchmarks.util;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Thilina Buddhika
 */
public class SineCurveLoadProfiler {

    private int timeSlice;  // How many milliseconds correspond to one degree
    private long startTimestamp;
    private Random random;
    private AtomicLong cycles;

    public SineCurveLoadProfiler(int timeSlice, int seed) {
        this.timeSlice = timeSlice;
        this.random = new Random(seed);
        this.startTimestamp = System.currentTimeMillis();
        this.cycles = new AtomicLong(0);
    }

    public int nextSleepInterval() {
        long timeFromStart = System.currentTimeMillis() - startTimestamp;
        cycles.set((timeFromStart / timeSlice) / 360);
        double angleInRadians = (timeFromStart / timeSlice % 360) * Math.PI / 180;
        return (int) (1 + (9 * 0.5 * (1 - Math.sin(angleInRadians))));
    }
}
