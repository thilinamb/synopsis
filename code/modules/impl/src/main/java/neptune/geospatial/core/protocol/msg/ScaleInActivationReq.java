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
public class ScaleInActivationReq extends ControlMessage {

    private String prefix;
    private String orginComputation;
    private long lastMessageSent;

    public ScaleInActivationReq() {
        super(ProtocolTypes.SCALE_IN_ACTIVATION_REQ);
    }

    public ScaleInActivationReq(String prefix, String orginComputation, long lastMessageSent) {
        super(ProtocolTypes.SCALE_IN_ACTIVATION_REQ);
        this.prefix = prefix;
        this.orginComputation = orginComputation;
        this.lastMessageSent = lastMessageSent;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.orginComputation = dis.readUTF();
        this.lastMessageSent = dis.readLong();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.orginComputation);
        dos.writeLong(this.lastMessageSent);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getOrginComputation() {
        return orginComputation;
    }

    public long getLastMessageSent() {
        return lastMessageSent;
    }
}
