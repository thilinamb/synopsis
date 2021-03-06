package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Calculate the cumulative memory consumption at a given ts
 *
 * @author Thilina Buddhika
 */
public class CumulativeMemoryUsageProcessor implements MetricProcessor {

    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "cumul-memory-usage";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        double cumulMemoryConsumption = 0.0;
        for (String instanceId : metricData.keySet()) {
            double val = metricData.get(instanceId)[StatConstants.RegistryIndices.PROC_MEMORY];
            if (val != -1) {
                cumulMemoryConsumption += val;
            }
        }
        buffW.write(ts + "," + String.format("%.3f", cumulMemoryConsumption / (1024 * 1024)));
    }
}
