package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PersistStateRequest extends ControlMessage {

    private long checkpointId;
    private String clientAddr;
    private boolean sendBackPrefixTree;

    public PersistStateRequest() {
        super(ProtocolTypes.PERSIST_STATE_REQ);
    }

    public PersistStateRequest(long checkpointId, String clientAddr, boolean sendBackPrefixTree) {
        super(ProtocolTypes.PERSIST_STATE_REQ);
        this.checkpointId = checkpointId;
        this.clientAddr = clientAddr;
        this.sendBackPrefixTree = sendBackPrefixTree;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.checkpointId = dataInputStream.readLong();
        this.clientAddr = dataInputStream.readUTF();
        this.sendBackPrefixTree = dataInputStream.readBoolean();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(this.checkpointId);
        dataOutputStream.writeUTF(this.clientAddr);
        dataOutputStream.writeBoolean(this.sendBackPrefixTree);
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public boolean isSendBackPrefixTree() {
        return sendBackPrefixTree;
    }
}
