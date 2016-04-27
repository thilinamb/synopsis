package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class CumulProcessedMsgCountProcessor implements MetricProcessor {
    @Override
    public boolean isForIngesters() {
        return true;
    }

    @Override
    public String getOutputFileName() {
        return "cumul-ingested-count";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, DataOutputStream dos) throws IOException {
        double totalProcessed = 0.0;
        for (double[] metrics : metricData.values()) {
            double val = metrics[StatConstants.RegistryIndices.ING_SENT_MSG_COUNT];
            if (val != -1) {
                totalProcessed += val;
            }
        }
        dos.writeLong(ts);
        dos.writeDouble(totalProcessed);
    }
}
