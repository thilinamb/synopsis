package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class SketchletCountHistogramGenerator implements MetricProcessor {
    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "sketchlet-count-dist";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        buffW.write(":" + ts + "\n");
        for (String instance : metricData.keySet()) {
            double[] metrics = metricData.get(instance);
            double val = metrics[StatConstants.RegistryIndices.PROC_LOCALLY_PROCESSED_PREF_COUNT];
            if (val != -1) {
                String metricStr = instance + "," + String.valueOf(val) +
                        "," +
                        metrics[StatConstants.RegistryIndices.PROC_PREFIX_LENGTH] +
                        "," +
                        metrics[StatConstants.RegistryIndices.PROC_THROUGHPUT] +
                        "," +
                        metrics[StatConstants.RegistryIndices.PROC_MEMORY] +
                        "," +
                        metrics[StatConstants.RegistryIndices.PROC_BACKLOG];
                buffW.write(metricStr);
                buffW.write('\n');
            }
        }
    }
}
