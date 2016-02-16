package neptune.geospatial.graph.operators;

import neptune.geospatial.core.computations.GeoSpatialStreamProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import org.apache.log4j.Logger;

/**
 * This is a temporary operator used to count
 * the number of records in order to make sure
 * initial setup works.
 *
 * @author Thilina Buddhika
 */
public class RecordCounter extends GeoSpatialStreamProcessor {

    private Logger logger = Logger.getLogger(RecordCounter.class);
    private int counter = 0;

    @Override
    protected void process(GeoHashIndexedRecord record) {
        if (++counter % 100000 == 0) {
            logger.info(String.format("[%s] Record received. Counter: %d", getInstanceIdentifier(), counter));
        }
    }
}
