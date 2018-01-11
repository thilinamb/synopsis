package neptune.geospatial.benchmarks.airquality;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.FeatureHierarchy;
import io.sigpipe.sing.graph.GraphException;
import io.sigpipe.sing.graph.Path;
import io.sigpipe.sing.graph.Sketch;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.util.ReducedTestConfiguration;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.NOAADataIngester;
import neptune.geospatial.graph.operators.SketchProcessor;

/**
 * @author Thilina Buddhika
 */
public class AirQualitySketchProcessor extends SketchProcessor {

    @Override
    protected void initSketch() {
        try {
            hierarchy = new FeatureHierarchy();
            for (String featureName : Configuration.FEATURE_NAMES) {
                hierarchy.addFeature(featureName, FeatureType.FLOAT);
            }
            hierarchy.addFeature("location", FeatureType.STRING);
            this.sketch = new Sketch(hierarchy);
            this.diff = new Sketch(hierarchy);

            for (String featureName : Configuration.FEATURE_NAMES) {
                activeFeatures.add(featureName);
            }
        } catch (GraphException e) {
            System.out.println("Could not initialize sketch graph hierarchy");
            e.printStackTrace();
        }
    }

    @Override
    protected synchronized void process(GeoHashIndexedRecord event) {
        Metadata eventMetadata = null;
        try {
            byte[] payload = event.getPayload();
            eventMetadata = Serializer.deserialize(Metadata.class, payload);
        } catch (Exception e) {
            System.out.println("Could not deserialize event payload");
            e.printStackTrace();
        }

        try {
            Path path = new Path(this.activeFeatures.size() + 1);
            for (Feature f : eventMetadata.getAttributes()) {
                String featureName = f.getName();
                if (activeFeatures.contains(featureName) == false) {
                    continue;
                }

                Quantizer q = Configuration.quantizers.get(featureName);
                if (q == null) {
                    continue;
                }

                Feature quantizedFeature = q.quantize(f);
                path.add(new Feature(f.getName().intern(), quantizedFeature));
            }

            String shortLocation = event.getGeoHash().substring(0, NOAADataIngester.PRECISION);
            path.add(new Feature("location", shortLocation));
            this.sketch.addPath(path, eventMetadata);
            this.diff.addPath(path, eventMetadata);
        } catch (Exception e) {
            System.out.println("Failed to insert graph path");
            e.printStackTrace();
        }
    }
}
