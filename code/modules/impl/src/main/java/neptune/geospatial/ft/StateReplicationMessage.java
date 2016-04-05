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

    private String computationId;
    private byte stateType;
    private byte[] serializedState;

    public StateReplicationMessage() {
    }

    public StateReplicationMessage(String computationId, byte type, byte[] serializedState) {
        this.computationId = computationId;
        this.stateType = type;
        this.serializedState = serializedState;
    }

    @Override
    protected void readValues(DataInputStream dataInputStream) throws IOException {
        this.computationId = dataInputStream.readUTF();
        this.stateType = dataInputStream.readByte();
        this.serializedState = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.serializedState);
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.computationId);
        dataOutputStream.writeByte(this.stateType);
        dataOutputStream.writeInt(this.serializedState.length);
        dataOutputStream.write(this.serializedState);
    }

    public String getComputationId() {
        return computationId;
    }

    public byte getType() {
        return stateType;
    }

    public byte[] getSerializedState() {
        return serializedState;
    }
}
