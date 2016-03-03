package neptune.geospatial.benchmarks.dynamicscaling;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryUpdatedListener;
import neptune.geospatial.hazelcast.type.SketchLocation;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Monitors the cluster for scaling in/out activities and logs them.
 *
 * @author Thilina Buddhika
 */
public class DynamicScalingMonitor implements EntryUpdatedListener<String, SketchLocation> {

    private final Logger logger = Logger.getLogger(DynamicScalingMonitor.class);
    private int activeComputationCount;
    private BufferedWriter bufferedWriter;

    public DynamicScalingMonitor(int activeComputationCount) {
        this.activeComputationCount = activeComputationCount;
        try {
            this.bufferedWriter = new BufferedWriter(new FileWriter("/tmp/instance-count.stat"));
            writeToBuffer();
        } catch (IOException e) {
            logger.error("Error opening file for writing instance monitor data.", e);
        }
    }

    @Override
    public void entryUpdated(EntryEvent<String, SketchLocation> entryEvent) {
        try {
            SketchLocation sketchLocation = entryEvent.getValue();
            if (sketchLocation.getMode() == SketchLocation.MODE_SCALE_IN) {
                synchronized (this) {
                    activeComputationCount--;
                    writeToBuffer();
                }
            } else if (sketchLocation.getMode() == SketchLocation.MODE_SCALE_OUT) {
                synchronized (this) {
                    activeComputationCount++;
                    writeToBuffer();
                }
            }
        } catch (IOException e) {
            logger.error("Error writing to the file from instance monitor.", e);
        }
    }

    private void writeToBuffer() throws IOException {
        bufferedWriter.write(System.currentTimeMillis() + "," + this.activeComputationCount + "\n");
        bufferedWriter.flush();
    }
}
