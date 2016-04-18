package neptune.geospatial.benchmarks.ft;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Monitors the cluster for scaling in/out activities and logs them.
 *
 * @author Thilina Buddhika
 */
public class DynamicScalingMonitor implements ItemListener<Integer> {

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

    private void writeToBuffer() throws IOException {
        bufferedWriter.write(System.currentTimeMillis() + "," + this.activeComputationCount + "\n");
        bufferedWriter.flush();
    }


    @Override
    public void itemAdded(ItemEvent<Integer> itemEvent) {
        synchronized (this) {
            activeComputationCount += itemEvent.getItem();
            try {
                writeToBuffer();
            } catch (IOException e) {
                logger.error("Error writing the scaling data into the buffer.", e);
            }
        }
    }

    @Override
    public void itemRemoved(ItemEvent<Integer> itemEvent) {
        throw new UnsupportedOperationException("The method itemRemoved is not supported in DynamicScalingMonitor class");
    }
}
