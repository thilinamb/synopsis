package neptune.geospatial.core.computations.scalingctxt;

/**
 * Full qualified address of a computation in the cluster.
 *
 * @author Thilina Buddhika
 */
public class FullQualifiedComputationAddr {
    private String ctrlEndpointAddr;
    private String computationId;

    public FullQualifiedComputationAddr(String ctrlEndpointAddr, String computationId) {
        this.ctrlEndpointAddr = ctrlEndpointAddr;
        this.computationId = computationId;
    }

    public String getCtrlEndpointAddr() {
        return ctrlEndpointAddr;
    }

    public String getComputationId() {
        return computationId;
    }
}
