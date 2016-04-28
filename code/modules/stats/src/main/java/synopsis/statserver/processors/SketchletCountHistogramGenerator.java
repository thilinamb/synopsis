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
        StringBuilder sBuilder = new StringBuilder(ts + "");
        for(String instance : metricData.keySet()){
            double val = metricData.get(instance)[StatConstants.RegistryIndices.PROC_LOCALLY_PROCESSED_PREF_COUNT];
            if(val != -1){
                sBuilder.append(",");
                sBuilder.append(val);
            }
        }
        buffW.write(sBuilder.toString());
    }
}
