package neptune.geospatial.benchmarks.dynamicscaling;

import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

/**
 * @author Thilina Buddhika
 */
public class StreamProcessor extends AbstractGeoSpatialStreamProcessor {

    @Override
    protected void process(GeoHashIndexedRecord event) {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {

        }
    }

    @Override
    public byte[] split(String prefix) {
        return new byte[0];
    }

    @Override
    public void merge(String prefix, byte[] serializedSketch) {

    }
}
