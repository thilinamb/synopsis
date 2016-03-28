package neptune.geospatial.core.protocol.msg.scaleout;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class DeploymentAck extends ControlMessage{

    private String instanceIdentifier;

    public DeploymentAck() {
        super(ProtocolTypes.DEPLOYMENT_ACK);
    }

    public DeploymentAck(String instanceIdentifier) {
        super(ProtocolTypes.DEPLOYMENT_ACK);
        this.instanceIdentifier = instanceIdentifier;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.instanceIdentifier = dis.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeUTF(this.instanceIdentifier);
    }

    public String getInstanceIdentifier() {
        return instanceIdentifier;
    }
}
