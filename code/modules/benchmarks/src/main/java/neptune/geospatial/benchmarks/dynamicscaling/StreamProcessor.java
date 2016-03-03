package neptune.geospatial.benchmarks.dynamicscaling;

import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

/**
 * @author Thilina Buddhika
 */
public class StreamProcessor extends AbstractGeoSpatialStreamProcessor {

    @Override
    protected void process(GeoHashIndexedRecord event) {
        // do nothing for the moment.
    }

    @Override
    public byte[] split(String prefix) {
        return new byte[0];
    }

    @Override
    public void merge(String prefix, byte[] serializedSketch) {

    }
}
