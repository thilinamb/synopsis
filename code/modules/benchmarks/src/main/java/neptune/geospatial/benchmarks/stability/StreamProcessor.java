package neptune.geospatial.benchmarks.stability;

import com.hazelcast.core.IQueue;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class StreamProcessor extends AbstractGeoSpatialStreamProcessor {

    private BufferedWriter scaleOutBuffW = null;
    private BufferedWriter scaleInBuffW = null;
    private BufferedWriter throughputBuffW = null;
    // for calculating throughput
    private long count = 0;
    private long lastTs = -1;
    private long flushCount = 0;

    @Override
    protected void process(GeoHashIndexedRecord event) {
        if (scaleOutBuffW == null && scaleInBuffW == null) {
            try {
                scaleOutBuffW = new BufferedWriter(new FileWriter("/tmp/scale_out.stat"));
                scaleInBuffW = new BufferedWriter(new FileWriter("/tmp/scale_in.stat"));
                throughputBuffW = new BufferedWriter(new FileWriter("/tmp/throughput.stat"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(5);
            count++;
            long now = System.currentTimeMillis();
            if (lastTs == -1) lastTs = now;
            else {
                if ((now - lastTs) >= 1000) {
                    try {
                        throughputBuffW.write(now + "," + ((count * 1.0) * 1000 / (now - lastTs)) + "\n");
                        if (++flushCount % 5 == 0) {
                            throughputBuffW.flush();
                        }
                        count = 0;
                        lastTs = now;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (InterruptedException e) {

        }
    }

    @Override
    public byte[] split(String prefix) {
        return new byte[0];
    }

    @Override
    public void merge(String prefix, byte[] serializedSketch) {

    }

    @Override
    public void onStartOfScaleOut() {
        try {
            scaleOutBuffW.write("start," + System.currentTimeMillis() + "\n");
            scaleOutBuffW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStartOfScaleIn() {
        try {
            scaleInBuffW.write("start," + System.currentTimeMillis() + "\n");
            scaleInBuffW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onSuccessfulScaleOut(List<String> prefixes) {
        try {
            scaleOutBuffW.write("end," + System.currentTimeMillis() + "\n");
            scaleOutBuffW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        IQueue<Integer> scalingMonitoringQueue = getHzInstance().getQueue("scaling-monitor");
        scalingMonitoringQueue.add(prefixes.size());
    }

    @Override
    public void onSuccessfulScaleIn(List<String> prefixes) {
        try {
            scaleInBuffW.write("end," + System.currentTimeMillis() + "\n");
            scaleInBuffW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        IQueue<Integer> scaleMonitorQueue = getHzInstance().getQueue("scaling-monitor");
        scaleMonitorQueue.add(-1 * prefixes.size());
    }
}
