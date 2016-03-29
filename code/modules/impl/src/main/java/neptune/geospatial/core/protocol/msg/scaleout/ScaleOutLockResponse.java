package neptune.geospatial.core.protocol.msg.scaleout;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutLockResponse extends ControlMessage {

    private String key;
    private String sourceComputation;
    private String targetComputation;
    private boolean lockAcquired;

    public ScaleOutLockResponse() {
        super(ProtocolTypes.SCALE_OUT_LOCK_RESP);
    }

    public ScaleOutLockResponse(String key, String sourceComputation, String targetComputation, boolean lockAcquired) {
        super(ProtocolTypes.SCALE_OUT_LOCK_RESP);
        this.key = key;
        this.targetComputation = targetComputation;
        this.sourceComputation = sourceComputation;
        this.lockAcquired = lockAcquired;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.key = dataInputStream.readUTF();
        this.sourceComputation = dataInputStream.readUTF();
        this.targetComputation = dataInputStream.readUTF();
        this.lockAcquired = dataInputStream.readBoolean();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.key);
        dataOutputStream.writeUTF(this.sourceComputation);
        dataOutputStream.writeUTF(this.targetComputation);
        dataOutputStream.writeBoolean(this.lockAcquired);
    }

    public String getKey() {
        return key;
    }

    public String getSourceComputation() {
        return sourceComputation;
    }

    public String getTargetComputation() {
        return targetComputation;
    }

    public boolean isLockAcquired() {
        return lockAcquired;
    }
}
