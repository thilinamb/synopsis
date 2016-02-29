package neptune.geospatial.core.computations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.graph.FeatureTypeMismatchException;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.serialization.Serializer;

import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

public class SketchProcessor extends AbstractGeoSpatialStreamProcessor {

    private Map<String, Sketch> sketches = new HashMap<>();

    protected void process(GeoHashIndexedRecord event) {

        String location = event.getGeoHash();
        Sketch sketch = sketches.get(location);
        if (sketch == null) {
            sketch = new Sketch();
            sketches.put(location, sketch);
        }

        Metadata eventMetadata = null;
        try {
            byte[] payload = event.getPayload();
            eventMetadata = Serializer.deserialize(Metadata.class, payload);
        } catch (Exception e) {
            //TODO log this
            System.out.println("Could not deserialize event payload");
            e.printStackTrace();
        }

        // TODO: preprocessing of incoming metadata goes here. remove features,
        // etc.
        Path path = new Path(eventMetadata.getAttributes().toArray());

        try {
            sketch.addPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] split(String prefix) {

        return null;
    }

    public void merge(String prefix, byte[] serializedSketch) {

    }

}
