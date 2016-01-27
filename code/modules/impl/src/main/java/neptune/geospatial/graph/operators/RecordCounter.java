package neptune.geospatial.graph.operators;

import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import ds.granules.streaming.core.exception.StreamingGraphConfigurationException;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import org.apache.log4j.Logger;

/**
 * This is a temporary operator used to count
 * the number of records in order to make sure
 * initial setup works.
 *
 * @author Thilina Buddhika
 */
public class RecordCounter extends StreamProcessor {

    private Logger logger = Logger.getLogger(RecordCounter.class);
    private int counter = 0;

    @Override
    public void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        GeoHashIndexedRecord record = (GeoHashIndexedRecord)streamEvent;
        if(++counter % 100000 == 0){
            logger.info(String.format("Record received. Counter: %d Hash: %s Timestamp: %d", counter,
                    record.getGeoHash(), record.getTsIngested()));
        }
    }

    @Override
    protected void declareOutputStreams() throws StreamingGraphConfigurationException {
        // leaf node of the graph. no outgoing edges.
    }
}
