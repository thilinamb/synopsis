package neptune.geospatial.ft;

import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

import java.util.Queue;

/**
 * @author Thilina Buddhika
 */
public class PendingUpstreamCheckpoint {

    private long checkpointId;
    private int pendingChildAcks;
    private Queue<GeoHashIndexedRecord> backup;
    private boolean success = true;

    public PendingUpstreamCheckpoint(long checkpointId, int pendingChildAcks, Queue<GeoHashIndexedRecord> backup) {
        this.checkpointId = checkpointId;
        this.pendingChildAcks = pendingChildAcks;
        this.backup = backup;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public int getPendingChildAcks() {
        return pendingChildAcks;
    }

    public Queue<GeoHashIndexedRecord> getBackup() {
        return backup;
    }

    public int recordChildAck(boolean status){
        success = success && status;
        return --pendingChildAcks;
    }

    public boolean isSuccess() {
        return success;
    }
}
