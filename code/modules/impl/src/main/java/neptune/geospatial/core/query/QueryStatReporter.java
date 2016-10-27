package neptune.geospatial.core.query;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class QueryStatReporter {

    private static QueryStatReporter instance = new QueryStatReporter();
    private Map<String, Long> queryCounter = new HashMap<>();

    private QueryStatReporter() {
        // singleton: private constructor
    }

    public static QueryStatReporter getInstance() {
        return instance;
    }

    public synchronized void record(String compId, long delta) {
        long counter = 0;
        if (queryCounter.containsKey(compId)) {
            counter = queryCounter.get(compId);
        }
        counter += delta;
        queryCounter.put(compId, counter);
    }

    public synchronized long getProcessedQueryCount(String compId) {
        if(queryCounter.containsKey(compId)) {
            return queryCounter.get(compId);
        } else {
            return 0;
        }
    }
}
