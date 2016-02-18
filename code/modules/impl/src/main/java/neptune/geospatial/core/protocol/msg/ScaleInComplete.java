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
    private String targetComputation;

    public ScaleInComplete() {
        super(ProtocolTypes.SCALE_IN_COMPLETE);
    }

    public ScaleInComplete(String prefix, String targetComputation) {
        super(ProtocolTypes.SCALE_IN_COMPLETE);
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
