package neptune.geospatial.core.computations;

import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

public class SketchProcessor extends AbstractGeoSpatialStreamProcessor {

    protected void process(GeoHashIndexedRecord event) {

    }

    public byte[] split(String prefix) {

        return null;
    }

    public void merge(String prefix, byte[] serializedSketch) {

    }

}
