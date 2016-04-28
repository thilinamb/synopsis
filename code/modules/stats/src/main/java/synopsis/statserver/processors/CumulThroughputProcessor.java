package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class CumulThroughputProcessor implements MetricProcessor {
    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "cumul-throughput";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        double cumulThroughput = 0.0;
        for (double[] metrics : metricData.values()) {
            double val = metrics[StatConstants.RegistryIndices.PROC_THROUGHPUT];
            if (val != -1) {
                cumulThroughput += val;
            }
        }
        buffW.write(ts + "," + String.format("%.3f", cumulThroughput));
    }
}
