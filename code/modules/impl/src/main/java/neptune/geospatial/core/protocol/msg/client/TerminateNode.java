package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class TerminateNode extends ControlMessage{

    public TerminateNode() {
        super(ProtocolTypes.TERMINATE_NODE);
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {

    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {

    }
}
