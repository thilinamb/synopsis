package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class QueryThroughputProcessor implements MetricProcessor {

    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "cumul-query-throughput";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        double cumulQueryThroughput = 0.0;
        for (String instanceId : metricData.keySet()) {
            double val = metricData.get(instanceId)[StatConstants.RegistryIndices.PROC_QUERY_THROUGHPUT];
            cumulQueryThroughput += val;
        }
        buffW.write(ts + "," + String.format("%.3f", cumulQueryThroughput));
    }
}
