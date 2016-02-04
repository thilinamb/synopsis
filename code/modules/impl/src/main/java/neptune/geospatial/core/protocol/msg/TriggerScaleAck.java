package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class TriggerScaleAck extends ControlMessage {

    private String inResponseTo;
    private boolean status;

    public TriggerScaleAck() {
        super(ProtocolTypes.TRIGGER_SCALING_ACK);
    }

    public TriggerScaleAck(String inResponseTo, boolean status) {
        super(ProtocolTypes.TRIGGER_SCALING_ACK);
        this.inResponseTo = inResponseTo;
        this.status = status;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.inResponseTo = dis.readUTF();
        this.status = dis.readBoolean();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.inResponseTo);
        dos.writeBoolean(this.status);
    }

    public String getInResponseTo() {
        return inResponseTo;
    }

    public boolean isStatus() {
        return status;
    }
}
