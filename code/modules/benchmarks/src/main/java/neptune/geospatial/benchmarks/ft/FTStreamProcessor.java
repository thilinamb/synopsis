package neptune.geospatial.benchmarks.ft;

import neptune.geospatial.ft.StateReplicaProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.SketchProcessor;
import org.apache.log4j.Logger;

import java.io.File;

/**
 * @author Thilina Buddhika
 */
public class FTStreamProcessor extends SketchProcessor {

    private int processedRecordCount = 0;
    private boolean backup = false;
    private Logger logger = Logger.getLogger(FTStreamProcessor.class);

    @Override
    protected void process(GeoHashIndexedRecord event) {
        synchronized (this) {
            if(processedRecordCount == 0){
                File stateStorageDir = new File(StateReplicaProcessor.CHECK_PT_STORAGE_LOC);
                if(stateStorageDir.exists() && stateStorageDir.isDirectory()){
                    File[] files = stateStorageDir.listFiles();
                    if(files != null && files.length > 0){
                        long t1 = System.currentTimeMillis();
                        populateSketch(StateReplicaProcessor.CHECK_PT_STORAGE_LOC);
                        long t2 = System.currentTimeMillis();
                        logger.info("Loaded serialized sketch from the disk. Time elapsed: " + (t2 - t1));
                        backup = true;
                    }
                }
            }
            processedRecordCount++;
            if(!backup) {
                try {
                    super.process(event);
                } catch (Exception e) {
                    logger.error("Processing Error! checkpoint id: " + event.getCheckpointId() + ", payload size: " + event.getPayload().length);
                }
            } else {
                logger.info("Secondary node started received data.");
            }
        }
    }
}
