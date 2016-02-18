package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class StateTransferMsg extends ControlMessage {

    private String prefix;
    private byte[] serializedData;
    private String targetComputation;

    public StateTransferMsg() {
        super(ProtocolTypes.STATE_TRANSFER_MSG);
    }

    public StateTransferMsg(String prefix, byte[] serializedData, String targetComputation) {
        super(ProtocolTypes.STATE_TRANSFER_MSG);
        this.prefix = prefix;
        this.serializedData = serializedData;
        this.targetComputation = targetComputation;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        int serializedDataSize = dis.readInt();
        this.serializedData = new byte[serializedDataSize];
        dis.readFully(this.serializedData);
        this.targetComputation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeInt(this.serializedData.length);
        dos.write(this.serializedData);
        dos.writeUTF(this.targetComputation);
    }

    public String getPrefix() {
        return prefix;
    }

    public byte[] getSerializedData() {
        return serializedData;
    }

    public String getTargetComputation() {
        return targetComputation;
    }
}
