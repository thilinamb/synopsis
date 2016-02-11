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

    public ScaleInRequest() {
        super(ProtocolTypes.SCALE_IN_REQ);
    }

    public ScaleInRequest(String prefix, String initiator) {
        super(ProtocolTypes.SCALE_IN_REQ);
        this.prefix = prefix;
        this.initiator = initiator;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.initiator = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.initiator);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getInitiator() {
        return initiator;
    }
}
