package neptune.geospatial.ft.protocol;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class CheckpointAck extends ControlMessage {

    public static final boolean ACK_FROM_STATE_REPLICATOR = true;
    public static final boolean ACK_FROM_CHILD = false;
    public static final boolean STATUS_SUCCESS = true;
    public static final boolean STATUS_FAILURE = false;

    private boolean fromReplicator;
    private boolean success;
    private long checkpointId;
    private String targetComputation;

    public CheckpointAck() {
        super(ProtocolTypes.CHECKPOINT_ACK);
    }

    public CheckpointAck(boolean fromReplicator, boolean success, long checkpointId, String targetComputation) {
        super(ProtocolTypes.CHECKPOINT_ACK);
        this.fromReplicator = fromReplicator;
        this.success = success;
        this.checkpointId = checkpointId;
        this.targetComputation = targetComputation;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.fromReplicator = dataInputStream.readBoolean();
        this.success = dataInputStream.readBoolean();
        this.checkpointId = dataInputStream.readLong();
        this.targetComputation = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeBoolean(this.fromReplicator);
        dataOutputStream.writeBoolean(this.success);
        dataOutputStream.writeLong(this.checkpointId);
        dataOutputStream.writeUTF(this.targetComputation);
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getTargetComputation() {
        return targetComputation;
    }

    public boolean isFromReplicator() {
        return fromReplicator;
    }

    public boolean isSuccess() {
        return success;
    }
}
