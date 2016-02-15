package neptune.geospatial.core.protocol.msg;

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

    public ScaleInActivateReq() {
        super(ProtocolTypes.SCALE_IN_ACTIVATION_REQ);
    }

    public ScaleInActivateReq(String prefix, String targetComputation, long lastMessageSent) {
        super(ProtocolTypes.SCALE_IN_ACTIVATION_REQ);
        this.prefix = prefix;
        this.targetComputation = targetComputation;
        this.lastMessageSent = lastMessageSent;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.targetComputation = dis.readUTF();
        this.lastMessageSent = dis.readLong();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.targetComputation);
        dos.writeLong(this.lastMessageSent);
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
}
