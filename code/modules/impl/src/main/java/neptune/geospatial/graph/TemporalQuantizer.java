package neptune.geospatial.graph;

import java.time.*;

import static neptune.geospatial.util.RivuletUtil.epochToLocalDateTime;
import static neptune.geospatial.util.RivuletUtil.localDateTimeToEpoch;

/**
 * Discretizes the time axis into intervals.
 */
public class TemporalQuantizer {
    private final Duration interval;
    private LocalDateTime boundary;

    public TemporalQuantizer(Duration interval) {
        this.interval = interval;
    }

    /**
     * Return the first boundary after the given timestamp such that boundary > timestamp
     *
     * @param ts Epoch millisecond timestamp
     * @return Boundary as a epoch millisecond timestamp
     */
    public long getBoundary(long ts) {
        LocalDateTime dateTime = epochToLocalDateTime(ts);
        if (boundary == null) {
            boundary = dateTime.minusHours(dateTime.getHour()).minusMinutes(dateTime.getMinute()).minusSeconds(
                    dateTime.getSecond()).minusNanos(dateTime.getNano()); // initialize to the start of the day of the first timestamp
        }
        if (boundary.minus(interval).isAfter(dateTime)) { // an out of order event arrived after advancing the boundary
            LocalDateTime tempBoundary = boundary.minus(interval); // this is a rare case. So handle it explicitly by temporary reverting back the boundary to an older value
            while (tempBoundary.isAfter(dateTime)) {    // find the last boundary before the ts
                tempBoundary = tempBoundary.minus(interval);
            }
            return localDateTimeToEpoch(tempBoundary.plus(interval));
        }
        while (boundary.isBefore(dateTime)) {   // find the first boundary after the timestamp - this also handles the case of sparse/missing timestamps
            boundary = boundary.plus(interval);
        }
        return localDateTimeToEpoch(boundary);
    }

       /*public static void main(String[] args) {
        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(Duration.ofHours(6));
        long now = System.currentTimeMillis();
        System.out.println("Now: " + epochToLocalDateTime(now));
        long boundary = temporalQuantizer.getBoundary(now);
        System.out.println("Boundary: " + epochToLocalDateTime(boundary));
        now += (1000 * 60 * 60 * 2);
        System.out.println("Now 2: " + epochToLocalDateTime(now));
        System.out.println("Boundary 2: " + epochToLocalDateTime(temporalQuantizer.getBoundary(now)));
        now += (1000 * 60 * 60 * 2);
        System.out.println("Now 3: " + epochToLocalDateTime(now));
        System.out.println("Boundary 3: " + epochToLocalDateTime(temporalQuantizer.getBoundary(now)));

        System.out.println("Obsolete TS: " + epochToLocalDateTime(temporalQuantizer.getBoundary(System.currentTimeMillis())));
    }*/
}
