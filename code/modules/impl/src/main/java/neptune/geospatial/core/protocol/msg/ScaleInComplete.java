package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Inform the completion of a Scale-In operation to
 * the rest of the participants.
 *
 * @author Thilina Buddhika
 */
public class ScaleInComplete extends ControlMessage {

    private String prefix;

    public ScaleInComplete(int messageType) {
        super(ProtocolTypes.SCALE_IN_COMPLETE);
    }

    public ScaleInComplete(String prefix) {
        super(ProtocolTypes.SCALE_IN_COMPLETE);
        this.prefix = prefix;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
    }

    public String getPrefix() {
        return prefix;
    }
}
