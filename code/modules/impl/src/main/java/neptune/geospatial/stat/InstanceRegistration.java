package neptune.geospatial.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class InstanceRegistration extends StatisticsRecord{

    public InstanceRegistration(String instanceId) {
        super(StatMessageTypes.REGISTER, instanceId);
    }

    @Override
    public void unmarshall(DataInputStream dataInputStream) throws IOException {

    }

    @Override
    public void marshall(DataOutputStream dataOutputStream) throws IOException {

    }
}
