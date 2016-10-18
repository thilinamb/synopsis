package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PersistStateAck extends ControlMessage {

    private long checkpointId;
    private int computationCount;

    public PersistStateAck() {
        super(ProtocolTypes.PERSIST_STATE_ACK);
    }

    public PersistStateAck(int computationCount, long checkpointId) {
        super(ProtocolTypes.PERSIST_STATE_ACK);
        this.computationCount = computationCount;
        this.checkpointId = checkpointId;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.computationCount = dataInputStream.readInt();
        this.checkpointId = dataInputStream.readLong();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.computationCount);
        dataOutputStream.writeLong(this.checkpointId);
    }

    public int getComputationCount() {
        return computationCount;
    }

    public long getCheckpointId() {
        return checkpointId;
    }
}
