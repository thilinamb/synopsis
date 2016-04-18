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

    private boolean fromReplicator;
    private long checkpointId;
    private String targetComputation;

    public CheckpointAck() {
        super(ProtocolTypes.CHECKPOINT_ACK);
    }

    public CheckpointAck(boolean fromReplicator, long checkpointId, String targetComputation) {
        super(ProtocolTypes.CHECKPOINT_ACK);
        this.fromReplicator = fromReplicator;
        this.checkpointId = checkpointId;
        this.targetComputation = targetComputation;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.fromReplicator = dataInputStream.readBoolean();
        this.checkpointId = dataInputStream.readLong();
        this.targetComputation = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeBoolean(this.fromReplicator);
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
}
