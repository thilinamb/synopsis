package neptune.geospatial.core.protocol.msg.scalein;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleInLockRequest extends ControlMessage {

    private String prefix;
    private String sourceComputation;
    private String targetComputation;

    public ScaleInLockRequest() {
        super(ProtocolTypes.SCALE_IN_LOCK_REQ);
    }

    public ScaleInLockRequest(String prefix, String sourceComputation, String targetComputation) {
        super(ProtocolTypes.SCALE_IN_LOCK_REQ);
        this.prefix = prefix;
        this.sourceComputation = sourceComputation;
        this.targetComputation = targetComputation;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.prefix = dis.readUTF();
        this.sourceComputation = dis.readUTF();
        this.targetComputation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.sourceComputation);
        dos.writeUTF(this.targetComputation);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSourceComputation() {
        return sourceComputation;
    }

    public String getTargetComputation() {
        return targetComputation;
    }
}
