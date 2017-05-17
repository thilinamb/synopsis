package synopsis.statserver.processors;

import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class LatencyProcessor implements MetricProcessor {
    private double totalProcessed = 0;
    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "cumul-processed";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        //double sumLatency = 0.0;
        for(double[] metrics: metricData.values()){
             totalProcessed += metrics[2];
        }
        buffW.write(ts + "," + String.format("%.3f", totalProcessed));
    }
}
