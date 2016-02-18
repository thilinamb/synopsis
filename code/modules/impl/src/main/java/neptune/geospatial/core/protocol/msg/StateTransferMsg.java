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

    public static final boolean SCALE_IN = true;
    public static final boolean SCALE_OUT = false;

    private String prefix;
    private String keyPrefix;
    private byte[] serializedData;
    private String targetComputation;
    private boolean scaleType;

    public StateTransferMsg() {
        super(ProtocolTypes.STATE_TRANSFER_MSG);
    }

    public StateTransferMsg(String prefix, String keyPrefix, byte[] serializedData, String targetComputation,
                            boolean type) {
        super(ProtocolTypes.STATE_TRANSFER_MSG);
        this.prefix = prefix;
        this.keyPrefix = keyPrefix;
        this.serializedData = serializedData;
        this.targetComputation = targetComputation;
        this.scaleType = type;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.keyPrefix = dis.readUTF();
        int serializedDataSize = dis.readInt();
        this.serializedData = new byte[serializedDataSize];
        dis.readFully(this.serializedData);
        this.targetComputation = dis.readUTF();
        this.scaleType = dis.readBoolean();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.keyPrefix);
        dos.writeInt(this.serializedData.length);
        dos.write(this.serializedData);
        dos.writeUTF(this.targetComputation);
        dos.writeBoolean(this.scaleType);
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

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public boolean isScaleType() {
        return scaleType;
    }
}
