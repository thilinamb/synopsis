package neptune.geospatial.ft;

import ds.granules.streaming.core.datatype.AbstractStreamEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * {@link AbstractStreamEvent} implementation used to carry the
 * edit stream of the state
 *
 * @author Thilina Buddhika
 */
public class StateReplicationMessage extends AbstractStreamEvent {

    private long checkpointId;
    private byte stateType;
    private byte[] serializedState;
    private String primaryComp;
    private String primaryCompLocation;

    public StateReplicationMessage() {
    }

    public StateReplicationMessage(long checkpointId, byte type, byte[] serializedState,
                                   String primaryComp, String primaryCompLocation) {
        this.checkpointId = checkpointId;
        this.stateType = type;
        this.serializedState = serializedState;
        this.primaryComp = primaryComp;
        this.primaryCompLocation = primaryCompLocation;
    }

    @Override
    protected void readValues(DataInputStream dataInputStream) throws IOException {
        this.checkpointId = dataInputStream.readLong();
        this.stateType = dataInputStream.readByte();
        this.serializedState = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.serializedState);
        this.primaryComp = dataInputStream.readUTF();
        this.primaryCompLocation = dataInputStream.readUTF();
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(this.checkpointId);
        dataOutputStream.writeByte(this.stateType);
        dataOutputStream.writeInt(this.serializedState.length);
        dataOutputStream.write(this.serializedState);
        dataOutputStream.writeUTF(this.primaryComp);
        dataOutputStream.writeUTF(this.primaryCompLocation);
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public byte getType() {
        return stateType;
    }

    public byte[] getSerializedState() {
        return serializedState;
    }

    public String getPrimaryComp() {
        return primaryComp;
    }

    public String getPrimaryCompLocation() {
        return primaryCompLocation;
    }
}
