package neptune.geospatial.core.computations;


import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.StreamProcessor;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

/**
 * Stream processor specialized for geo-spatial data processing.
 * Specific computations may be implement by extending this
 * abstract class.
 *
 * @author Thilina Buddhika
 */
@SuppressWarnings("unused")
public abstract class GeoSpatialStreamProcessor extends StreamProcessor {

    @Override
    public final void onEvent(StreamEvent streamEvent) throws StreamingDatasetException {
        GeoHashIndexedRecord geoHashIndexedRecord = (GeoHashIndexedRecord) streamEvent;
        // preprocess each message
        preprocess(geoHashIndexedRecord);
        // perform the business logic
        onEvent(geoHashIndexedRecord);
    }

    /**
     * Implement the specific business logic to process each
     * <code>GeohashIndexedRecord</code> message.
     *
     * @param event <code>GeoHashIndexedRecord</code> element
     */
    protected abstract void onEvent(GeoHashIndexedRecord event);

    /**
     * Preprocess every record to extract meta-data such as triggering
     * scale out operations. This is prior to performing actual processing
     * on a message.
     *
     * @param record <code>GeoHashIndexedRecord</code> element
     */
    protected abstract void preprocess(GeoHashIndexedRecord record);

}




