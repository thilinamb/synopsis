package neptune.geospatial.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PeriodicInstanceMetrics extends StatisticsRecord {

    private boolean instanceType;
    private double[] metrics;

    public PeriodicInstanceMetrics(String instanceId, double[] metrics, boolean instanceType) {
        super(StatConstants.MessageTypes.PERIODIC_UPDATE, instanceId);
        this.instanceType = instanceType;
        this.metrics = metrics;
    }

    @Override
    public void unmarshall(DataInputStream dataInputStream) throws IOException {
        this.instanceType = dataInputStream.readBoolean();
        this.metrics = new double[dataInputStream.readInt()];
        for(int i = 0; i < metrics.length; i++){
            this.metrics[i] = dataInputStream.readDouble();
        }
    }

    @Override
    public void marshall(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeBoolean(this.instanceType);
        dataOutputStream.writeInt(this.metrics.length);
        for (double metric : this.metrics) {
            dataOutputStream.writeDouble(metric);
        }
    }

    public double[] getMetrics() {
        return metrics;
    }

    public boolean isInstanceType() {
        return instanceType;
    }
}
