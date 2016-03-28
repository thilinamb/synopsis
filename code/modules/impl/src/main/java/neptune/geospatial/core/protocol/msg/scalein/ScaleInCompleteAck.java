package neptune.geospatial.core.protocol.msg.scalein;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleInCompleteAck extends ControlMessage {

    private String prefix;
    private String targetComputation;

    public ScaleInCompleteAck() {
        super(ProtocolTypes.SCALE_IN_COMPLETE_ACK);
    }

    public ScaleInCompleteAck(String prefix, String targetComputation) {
        super(ProtocolTypes.SCALE_IN_COMPLETE_ACK);
        this.prefix = prefix;
        this.targetComputation = targetComputation;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.targetComputation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.targetComputation);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getTargetComputation() {
        return targetComputation;
    }
}
