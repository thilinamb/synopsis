package neptune.geospatial.gossip.type;

/**
 * @author Thilina Buddhika
 */
public class SketchLocation {

    private String computation;
    private String ctrlEndpoint;

    public SketchLocation(String computation, String ctrlEndpoint) {
        this.computation = computation;
        this.ctrlEndpoint = ctrlEndpoint;
    }

    public String getComputation() {
        return computation;
    }

    public String getCtrlEndpoint() {
        return ctrlEndpoint;
    }

    @Override
    public String toString() {
        return computation + " -> " + ctrlEndpoint;
    }
}
