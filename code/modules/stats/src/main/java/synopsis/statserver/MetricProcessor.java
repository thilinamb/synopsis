package synopsis.statserver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public interface MetricProcessor {

    boolean isForIngesters();

    String getOutputFileName();

    void process(Map<String, double[]> metricData, long ts, DataOutputStream dos) throws IOException;
}
