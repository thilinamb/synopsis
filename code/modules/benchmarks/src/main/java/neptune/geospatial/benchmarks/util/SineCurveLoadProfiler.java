package neptune.geospatial.benchmarks.util;

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

    public int nextSleepInterval() {
        long timeFromStart = System.currentTimeMillis() - startTimestamp;
        cycles.set((timeFromStart / timeSlice) / 360);
        double angleInRadians = (timeFromStart / timeSlice % 360) * Math.PI / 180;
        return (int) (3 + (9 * 0.5 * (1 - Math.sin(angleInRadians))));
    }
}
