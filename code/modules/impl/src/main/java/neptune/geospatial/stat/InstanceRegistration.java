package neptune.geospatial.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class InstanceRegistration extends StatisticsRecord{

    private boolean instanceType;

    public InstanceRegistration(String instanceId, boolean instanceType) {
        super(StatConstants.MessageTypes.REGISTER, instanceId);
        this.instanceType = instanceType;
    }

    @Override
    public void unmarshall(DataInputStream dataInputStream) throws IOException {
        this.instanceType = dataInputStream.readBoolean();
    }

    @Override
    public void marshall(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeBoolean(this.instanceType);
    }

    public boolean isInstanceType() {
        return instanceType;
    }
}
