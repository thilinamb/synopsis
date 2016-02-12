package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleInResponse extends ControlMessage {

    private boolean success;
    private String prefix;
    private String destinationComputation;
    private String originComputation;

    public ScaleInResponse(boolean success, String prefix, String destinationComputation, String originComputation) {
        super(ProtocolTypes.SCALE_IN_RESP);
        this.success = success;
        this.prefix = prefix;
        this.destinationComputation = destinationComputation;
        this.originComputation = originComputation;
    }

    public ScaleInResponse() {
        super(ProtocolTypes.SCALE_IN_RESP);
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.success = dis.readBoolean();
        this.prefix = dis.readUTF();
        this.destinationComputation = dis.readUTF();
        this.originComputation = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeBoolean(this.success);
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.destinationComputation);
        dos.writeUTF(this.originComputation);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getDestinationComputation() {
        return destinationComputation;
    }

    public String getOriginComputation() {
        return originComputation;
    }
}
