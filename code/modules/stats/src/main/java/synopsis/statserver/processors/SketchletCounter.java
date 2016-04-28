package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.MetricProcessor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class SketchletCounter implements MetricProcessor {
    @Override
    public boolean isForIngesters() {
        return false;
    }

    @Override
    public String getOutputFileName() {
        return "sketchlet-count";
    }

    @Override
    public void process(Map<String, double[]> metricData, long ts, BufferedWriter buffW) throws IOException {
        int uniqueSketchletCount = 0;
        for(String instance : metricData.keySet()){
            double val = metricData.get(instance)[StatConstants.RegistryIndices.PROC_LOCALLY_PROCESSED_PREF_COUNT];
            if(val != -1){
                uniqueSketchletCount++;
            }
        }
        buffW.write(ts + "," + uniqueSketchletCount);
    }
}
