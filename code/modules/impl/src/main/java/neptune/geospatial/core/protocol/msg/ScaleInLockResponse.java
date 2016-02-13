package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Response for a scale-in lock request
 *
 * @author Thilina Buddhika
 */
public class ScaleInLockResponse extends ControlMessage {

    private boolean success;
    private String prefix;
    private String computation;

    public ScaleInLockResponse() {
        super(ProtocolTypes.SCALE_IN_LOCK_RESP);
    }

    public ScaleInLockResponse(boolean success, String prefix, String computation) {
        super(ProtocolTypes.SCALE_IN_LOCK_RESP);
        this.success = success;
        this.prefix = prefix;
        this.computation = computation;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.success = dis.readBoolean();
        this.prefix = dis.readUTF();
        this.computation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeBoolean(this.success);
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.computation);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getComputation() {
        return computation;
    }
}
