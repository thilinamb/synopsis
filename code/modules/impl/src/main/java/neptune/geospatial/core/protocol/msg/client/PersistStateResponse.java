package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PersistStateResponse extends ControlMessage {

    private boolean success;
    private long checkpointId;
    private String storageLocation;
    private String computationId;
    private boolean containsPrefixTree;
    private byte[] prefixTree;

    public PersistStateResponse() {
        super(ProtocolTypes.PERSIST_STATE_RESP);
    }

    public PersistStateResponse(boolean success) {
        super(ProtocolTypes.PERSIST_STATE_RESP);
        this.success = success;
    }

    public PersistStateResponse(long checkpointId, String storageLocation, String computationId) {
        super(ProtocolTypes.PERSIST_STATE_RESP);
        this.success = true;
        this.checkpointId = checkpointId;
        this.storageLocation = storageLocation;
        this.computationId = computationId;
        this.containsPrefixTree = false;
    }

    public PersistStateResponse(long checkpointId, String storageLocation, String computationId, byte[] prefixTree) {
        super(ProtocolTypes.PERSIST_STATE_RESP);
        this.success = true;
        this.checkpointId = checkpointId;
        this.storageLocation = storageLocation;
        this.computationId = computationId;
        this.containsPrefixTree = true;
        this.prefixTree = prefixTree;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.success = dataInputStream.readBoolean();
        if(this.success) {
            this.checkpointId = dataInputStream.readLong();
            this.storageLocation = dataInputStream.readUTF();
            this.computationId = dataInputStream.readUTF();
            this.containsPrefixTree = dataInputStream.readBoolean();
            if (this.containsPrefixTree) {
                int prefTreeLength = dataInputStream.readInt();
                this.prefixTree = new byte[prefTreeLength];
                dataInputStream.readFully(this.prefixTree);
            }
        }
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeBoolean(this.success);
        if(this.success) {
            dataOutputStream.writeLong(this.checkpointId);
            dataOutputStream.writeUTF(this.storageLocation);
            dataOutputStream.writeUTF(this.computationId);
            dataOutputStream.writeBoolean(this.containsPrefixTree);
            if (this.containsPrefixTree) {
                dataOutputStream.write(this.prefixTree.length);
                dataOutputStream.write(this.prefixTree);
            }
        }
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getStorageLocation() {
        return storageLocation;
    }

    public String getComputationId() {
        return computationId;
    }

    public boolean isContainsPrefixTree() {
        return containsPrefixTree;
    }

    public byte[] getPrefixTree() {
        return prefixTree;
    }

    public boolean isSuccess() {
        return success;
    }
}
