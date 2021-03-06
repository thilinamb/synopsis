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
    private String originComputation;
    private boolean scaleType;
    private long lastMessageId;
    private String lastMessagePrefix;
    private boolean acked;
    private String streamType;

    public StateTransferMsg() {
        super(ProtocolTypes.STATE_TRANSFER_MSG);
    }

    public StateTransferMsg(String prefix, String keyPrefix, byte[] serializedData, String targetComputation,
                            String originComputation, boolean type, String streamType) {
        super(ProtocolTypes.STATE_TRANSFER_MSG);
        this.prefix = prefix;
        this.keyPrefix = keyPrefix;
        this.serializedData = serializedData;
        this.targetComputation = targetComputation;
        this.originComputation = originComputation;
        this.scaleType = type;
        this.streamType = streamType;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.keyPrefix = dis.readUTF();
        int serializedDataSize = dis.readInt();
        this.serializedData = new byte[serializedDataSize];
        dis.readFully(this.serializedData);
        this.targetComputation = dis.readUTF();
        this.originComputation = dis.readUTF();
        this.scaleType = dis.readBoolean();
        if (!scaleType) { // appears only with scale out messages.
            this.lastMessageId = dis.readLong();
            this.lastMessagePrefix = dis.readUTF();
            this.streamType = dis.readUTF();
        }
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.keyPrefix);
        dos.writeInt(this.serializedData.length);
        dos.write(this.serializedData);
        dos.writeUTF(this.targetComputation);
        dos.writeUTF(this.originComputation);
        dos.writeBoolean(this.scaleType);
        if (!scaleType) {   // appears only with scale out messages.
            dos.writeLong(this.lastMessageId);
            dos.writeUTF(this.lastMessagePrefix);
            dos.writeUTF(this.streamType);
        }
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

    public String getOriginComputation() {
        return originComputation;
    }

    public long getLastMessageId() {
        return lastMessageId;
    }

    public void setLastMessageId(long lastMessageId) {
        this.lastMessageId = lastMessageId;
    }

    public String getLastMessagePrefix() {
        return lastMessagePrefix;
    }

    public void setLastMessagePrefix(String lastMessagePrefix) {
        this.lastMessagePrefix = lastMessagePrefix;
    }

    public boolean isAcked() {
        return acked;
    }

    public void setAcked(boolean acked) {
        this.acked = acked;
    }

    public String getStreamType() {
        return streamType;
    }
}
