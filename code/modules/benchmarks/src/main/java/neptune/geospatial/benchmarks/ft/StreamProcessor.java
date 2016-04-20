package neptune.geospatial.benchmarks.ft;

import com.hazelcast.core.IQueue;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class StreamProcessor extends AbstractGeoSpatialStreamProcessor {

    @Override
    protected void process(GeoHashIndexedRecord event) {
        try {
            Thread.sleep(5);
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
    public void onSuccessfulScaleOut(List<String> prefixes) {
        IQueue<Integer> scalingMonitoringQueue = getHzInstance().getQueue("scaling-monitor");
        scalingMonitoringQueue.add(prefixes.size());
    }

    @Override
    public void onSuccessfulScaleIn(List<String> prefixes) {
        IQueue<Integer> scaleMonitorQueue = getHzInstance().getQueue("scaling-monitor");
        scaleMonitorQueue.add(-1 * prefixes.size());
    }
}
