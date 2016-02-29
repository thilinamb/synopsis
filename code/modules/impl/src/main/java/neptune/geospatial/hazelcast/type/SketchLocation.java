package neptune.geospatial.hazelcast.type;

/**
 * Location of a sketch: The computation and the endpoint of the Granules
 * Resource.
 *
 * @author Thilina Buddhika
 */
public class SketchLocation {

    public static final byte MODE_REGISTER_NEW_PREFIX = 1;
    public static final byte MODE_SCALE_IN = 2;
    public static final byte MODE_SCALE_OUT = 3;

    private byte mode;
    private String computation;
    private String ctrlEndpoint;

    public SketchLocation(String computation, String ctrlEndpoint, byte mode) {
        this.computation = computation;
        this.ctrlEndpoint = ctrlEndpoint;
        this.mode = mode;
    }

    public String getComputation() {
        return computation;
    }

    public String getCtrlEndpoint() {
        return ctrlEndpoint;
    }

    public byte getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return computation + " -> " + ctrlEndpoint;
    }
}
