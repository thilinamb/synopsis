package neptune.geospatial.stat;

import ds.granules.communication.direct.control.ControlMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public abstract class StatisticsRecord extends ControlMessage {

    private String instanceId;

    public StatisticsRecord(int messageType, String instanceId) {
        super(messageType);
        this.instanceId = instanceId;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.instanceId = dataInputStream.readUTF();
        unmarshall(dataInputStream);
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.instanceId);
        marshall(dataOutputStream);
    }

    public abstract void unmarshall(DataInputStream dataInputStream) throws IOException;

    public abstract void marshall(DataOutputStream dataOutputStream) throws IOException;

    public String getInstanceId() {
        return instanceId;
    }
}
