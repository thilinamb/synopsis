package neptune.geospatial.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PeriodicInstanceMetrics extends StatisticsRecord {

    private double[] metrics;

    public PeriodicInstanceMetrics(String instanceId, double[] metrics) {
        super(StatMessageTypes.PERIODIC_UPDATE, instanceId);
        this.metrics = metrics;
    }

    @Override
    public void unmarshall(DataInputStream dataInputStream) throws IOException {
        this.metrics = new double[dataInputStream.readInt()];
        for(int i = 0; i < metrics.length; i++){
            this.metrics[i] = dataInputStream.readDouble();
        }
    }

    @Override
    public void marshall(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.metrics.length);
        for(int i = 0; i < this.metrics.length; i++){
            dataOutputStream.writeDouble(this.metrics[i]);
        }
    }

    public double[] getMetrics() {
        return metrics;
    }
}
