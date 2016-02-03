package neptune.geospatial.core.computations;


import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

/**
 * @author Thilina Buddhika
 */
public abstract class GeoSpatialStreamProcessor extends StreamProcessor {



    @Override
    public void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        onEvent(geoHashIndexedRecord);
    }

    public abstract void onEvent(GeoHashIndexedRecord event);

}



