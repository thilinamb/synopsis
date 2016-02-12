package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleInRequest extends ControlMessage {

    private String prefix;
    private String initiator;
    private long lastMessageSent;

    public ScaleInRequest() {
        super(ProtocolTypes.SCALE_IN_REQ);
    }

    public ScaleInRequest(String prefix, String initiator, long lastMessageId) {
        super(ProtocolTypes.SCALE_IN_REQ);
        this.prefix = prefix;
        this.initiator = initiator;
        this.lastMessageSent = lastMessageId;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.initiator = dis.readUTF();
        this.lastMessageSent = dis.readLong();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.initiator);
        dos.writeLong(this.lastMessageSent);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getInitiator() {
        return initiator;
    }

    public long getLastMessageSent() {
        return lastMessageSent;
    }
}
