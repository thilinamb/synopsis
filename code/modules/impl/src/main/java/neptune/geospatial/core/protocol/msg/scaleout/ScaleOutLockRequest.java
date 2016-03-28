package neptune.geospatial.core.protocol.msg.scaleout;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Acquires the target node mutex before scaling out
 *
 * @author Thilina Buddhika
 */
public class ScaleOutLockRequest extends ControlMessage {

    private String sourceComputation;
    private String targetComputation;

    public ScaleOutLockRequest() {
        super(ProtocolTypes.SCALE_OUT_LOCK_REQ);
    }

    public ScaleOutLockRequest(String sourceComputation, String targetComputation) {
        super(ProtocolTypes.SCALE_OUT_LOCK_REQ);
        this.sourceComputation = sourceComputation;
        this.targetComputation = targetComputation;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.sourceComputation = dataInputStream.readUTF();
        this.targetComputation = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.sourceComputation);
        dataOutputStream.writeUTF(this.targetComputation);
    }

    public String getSourceComputation() {
        return sourceComputation;
    }

    public void setSourceComputation(String sourceComputation) {
        this.sourceComputation = sourceComputation;
    }

    public String getTargetComputation() {
        return targetComputation;
    }

    public void setTargetComputation(String targetComputation) {
        this.targetComputation = targetComputation;
    }
}
