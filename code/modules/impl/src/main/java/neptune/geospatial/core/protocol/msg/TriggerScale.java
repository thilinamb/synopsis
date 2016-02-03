package neptune.geospatial.core.protocol.msg;


import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Triggers scaling up/down at the controller.
 * ScalableStreamSource -> Deployer
 *
 * @author Thilina Buddhika
 */
public class TriggerScale extends ControlMessage {

    private String currentComputation;

    public TriggerScale() {
        super(ProtocolTypes.TRIGGER_SCALING);
    }

    public TriggerScale(String currentComputation) {
        super(ProtocolTypes.TRIGGER_SCALING);
        this.currentComputation = currentComputation;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.currentComputation = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.currentComputation);
    }

    public String getCurrentComputation() {
        return currentComputation;
    }
}
