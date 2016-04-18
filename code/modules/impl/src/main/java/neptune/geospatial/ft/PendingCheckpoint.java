package neptune.geospatial.ft;

/**
 * Monitors a pending checkpoint
 *
 * @author Thilina Buddhika
 */
public class PendingCheckpoint {

    private long checkpointId;
    private int stateReplicationAcks;
    private int childAcks;
    private String parentCompId;
    private String parentCompEndpoint;

    public PendingCheckpoint(long checkpointId, int stateReplicationAcks, int childAcks,
                             String parentCompId, String parentCompEndpoint) {
        this.checkpointId = checkpointId;
        this.stateReplicationAcks = stateReplicationAcks;
        this.childAcks = childAcks;
        this.parentCompId = parentCompId;
        this.parentCompEndpoint = parentCompEndpoint;
    }

    public int ackFromStateReplicationProcessor() {
        return --stateReplicationAcks;
    }

    public int ackFromChild() {
        return --childAcks;
    }

    public boolean isCheckpointCompleted() {
        return (childAcks == 0) && (stateReplicationAcks == 0);
    }

    public String getParentCompId() {
        return parentCompId;
    }

    public String getParentCompEndpoint() {
        return parentCompEndpoint;
    }

    public int getStateReplicationAcks() {
        return stateReplicationAcks;
    }

    public int getChildAcks() {
        return childAcks;
    }
}
