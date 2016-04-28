package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class BacklogCounter implements MetricProcessor{
    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "cumul-backlog";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        double cumulativeBacklogSize = 0.0;
        for(String instance: metricData.keySet()){
            double val = metricData.get(instance)[StatConstants.RegistryIndices.PROC_BACKLOG];
            if(val > 0){
                cumulativeBacklogSize += val;
            }
        }
        buffW.write(ts + "," + cumulativeBacklogSize);
    }
}
