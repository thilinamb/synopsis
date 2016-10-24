package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class UpdatePrefixTreeReq extends ControlMessage {

    private byte[] updatedPrefixTree;

    public UpdatePrefixTreeReq() {
        super(ProtocolTypes.UPDATE_PREFIX_TREE);
    }

    public UpdatePrefixTreeReq(byte[] updatedPrefixTree) {
        super(ProtocolTypes.UPDATE_PREFIX_TREE);
        this.updatedPrefixTree = updatedPrefixTree;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.updatedPrefixTree = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.updatedPrefixTree);
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.updatedPrefixTree.length);
        dataOutputStream.write(this.updatedPrefixTree);
    }

    public byte[] getUpdatedPrefixTree() {
        return updatedPrefixTree;
    }
}
