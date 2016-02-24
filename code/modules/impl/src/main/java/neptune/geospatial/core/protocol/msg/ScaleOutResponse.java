package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutResponse extends ControlMessage {

    private String inResponseTo;
    private boolean status;
    private String targetComputation;
    private String newComputationId;
    private String newLocationURL;

    public ScaleOutResponse() {
        super(ProtocolTypes.SCALE_OUT_RESP);
    }

    public ScaleOutResponse(String inResponseTo, String targetComputation, boolean status) {
        super(ProtocolTypes.SCALE_OUT_RESP);
        this.inResponseTo = inResponseTo;
        this.targetComputation = targetComputation;
        this.status = status;
    }

    public ScaleOutResponse(String inResponseTo, String targetComputation, boolean status, String newComputationId, String newLocationURL) {
        super(ProtocolTypes.SCALE_OUT_RESP);
        this.inResponseTo = inResponseTo;
        this.targetComputation = targetComputation;
        this.status = status;
        this.newComputationId = newComputationId;
        this.newLocationURL = newLocationURL;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.inResponseTo = dis.readUTF();
        this.targetComputation = dis.readUTF();
        this.status = dis.readBoolean();
        this.newComputationId = dis.readUTF();
        this.newLocationURL = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.inResponseTo);
        dos.writeUTF(this.targetComputation);
        dos.writeBoolean(this.status);
        dos.writeUTF(this.newComputationId);
        dos.writeUTF(this.newLocationURL);
    }

    public String getInResponseTo() {
        return inResponseTo;
    }

    public boolean isStatus() {
        return status;
    }

    public String getTargetComputation() {
        return targetComputation;
    }

    public String getNewComputationId() {
        return newComputationId;
    }

    public String getNewLocationURL() {
        return newLocationURL;
    }
}
