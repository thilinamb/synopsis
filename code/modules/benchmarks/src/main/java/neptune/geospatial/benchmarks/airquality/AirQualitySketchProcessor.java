package neptune.geospatial.benchmarks.airquality;

import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.FeatureHierarchy;
import io.sigpipe.sing.graph.GraphException;
import io.sigpipe.sing.graph.Sketch;
import neptune.geospatial.graph.operators.SketchProcessor;

/**
 * @author Thilina Buddhika
 */
public class AirQualitySketchProcessor extends SketchProcessor {
    public AirQualitySketchProcessor() {
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
}
