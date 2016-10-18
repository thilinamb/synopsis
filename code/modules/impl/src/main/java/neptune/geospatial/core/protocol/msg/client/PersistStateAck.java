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

    private int computationCount;

    public PersistStateAck() {
        super(ProtocolTypes.PERSIST_STATE_ACK);
    }

    public PersistStateAck(int computationCount) {
        super(ProtocolTypes.PERSIST_STATE_ACK);
        this.computationCount = computationCount;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.computationCount = dataInputStream.readInt();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.computationCount);
    }

    public int getComputationCount() {
        return computationCount;
    }
}
