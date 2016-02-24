package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutCompleteAck extends ControlMessage {

    private String key;
    private String prefix;
    private String targetComputation;

    public ScaleOutCompleteAck(String key, String prefix, String targetComputation) {
        super(ProtocolTypes.SCALE_OUT_COMPLETE_ACK);
        this.key = key;
        this.prefix = prefix;
        this.targetComputation = targetComputation;
    }

    public ScaleOutCompleteAck() {
        super(ProtocolTypes.SCALE_OUT_COMPLETE_ACK);
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.key = dis.readUTF();
        this.prefix = dis.readUTF();
        this.targetComputation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.key);
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.targetComputation);
    }

    public String getKey() {
        return key;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getTargetComputation() {
        return targetComputation;
    }
}
