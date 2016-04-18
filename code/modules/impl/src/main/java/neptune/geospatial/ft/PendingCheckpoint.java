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

    public PendingCheckpoint(long checkpointId, int stateReplicationAcks, int childAcks) {
        this.checkpointId = checkpointId;
        this.stateReplicationAcks = stateReplicationAcks;
        this.childAcks = childAcks;
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
}
