package neptune.geospatial.ft;

/**
 * Monitors a pending checkpoint
 *
 * @author Thilina Buddhika
 */
public class PendingCheckpoint {

    private long checkpointId;
    private int pendingStateReplicationAcks;
    private int pendingChildAcks;
    private String parentCompId;
    private String parentCompEndpoint;
    private boolean checkpointStatus = true;

    public PendingCheckpoint(long checkpointId, int pendingStateReplicationAcks, int pendingChildAcks,
                             String parentCompId, String parentCompEndpoint) {
        this.checkpointId = checkpointId;
        this.pendingStateReplicationAcks = pendingStateReplicationAcks;
        this.pendingChildAcks = pendingChildAcks;
        this.parentCompId = parentCompId;
        this.parentCompEndpoint = parentCompEndpoint;
    }

    public int ackFromStateReplicationProcessor() {
        return --pendingStateReplicationAcks;
    }

    public int ackFromChild() {
        return --pendingChildAcks;
    }

    public boolean isCheckpointCompleted() {
        return (pendingChildAcks == 0) && (pendingStateReplicationAcks == 0);
    }

    public String getParentCompId() {
        return parentCompId;
    }

    public String getParentCompEndpoint() {
        return parentCompEndpoint;
    }

    public int getPendingStateReplicationAcks() {
        return pendingStateReplicationAcks;
    }

    public int getPendingChildAcks() {
        return pendingChildAcks;
    }

    public boolean updateCheckpointStatus(boolean status){
        checkpointStatus = checkpointStatus && status;
        return checkpointStatus;
    }
}
