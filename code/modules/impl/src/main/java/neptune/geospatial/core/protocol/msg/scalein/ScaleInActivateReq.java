package neptune.geospatial.core.protocol.msg.scalein;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Message sent after acquiring all the locks from the child
 * nodes.
 *
 * @author Thilina Buddhika
 */
public class ScaleInActivateReq extends ControlMessage {

    private String prefix;
    private String targetComputation;
    private long lastMessageSent;
    private String lastGeoHashSent;
    private int currentPrefixLength;
    private String originNodeOfScalingOperation;
    private String originComputationOfScalingOperation;

    public ScaleInActivateReq() {
        super(ProtocolTypes.SCALE_IN_ACTIVATION_REQ);
    }

    public ScaleInActivateReq(String prefix, String targetComputation, long lastMessageSent, String lastGeoHashSent,
                              int currentPrefixLength, String originNodeOfScalingOperation, String originComputationOfScalingOperation) {
        super(ProtocolTypes.SCALE_IN_ACTIVATION_REQ);
        this.prefix = prefix;
        this.targetComputation = targetComputation;
        this.lastMessageSent = lastMessageSent;
        this.lastGeoHashSent = lastGeoHashSent;
        this.currentPrefixLength = currentPrefixLength;
        this.originNodeOfScalingOperation = originNodeOfScalingOperation;
        this.originComputationOfScalingOperation = originComputationOfScalingOperation;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.targetComputation = dis.readUTF();
        this.lastMessageSent = dis.readLong();
        this.lastGeoHashSent = dis.readUTF();
        this.currentPrefixLength = dis.readInt();
        this.originNodeOfScalingOperation = dis.readUTF();
        this.originComputationOfScalingOperation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.targetComputation);
        dos.writeLong(this.lastMessageSent);
        dos.writeUTF(this.lastGeoHashSent);
        dos.writeInt(this.currentPrefixLength);
        dos.writeUTF(this.originNodeOfScalingOperation);
        dos.writeUTF(this.originComputationOfScalingOperation);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getTargetComputation() {
        return targetComputation;
    }

    public long getLastMessageSent() {
        return lastMessageSent;
    }

    public String getOriginNodeOfScalingOperation() {
        return originNodeOfScalingOperation;
    }

    public String getOriginComputationOfScalingOperation() {
        return originComputationOfScalingOperation;
    }

    public String getLastGeoHashSent() {
        return lastGeoHashSent;
    }

    public int getCurrentPrefixLength() {
        return currentPrefixLength;
    }
}
