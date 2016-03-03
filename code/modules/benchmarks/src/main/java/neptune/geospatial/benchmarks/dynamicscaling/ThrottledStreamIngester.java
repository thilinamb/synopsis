package neptune.geospatial.benchmarks.dynamicscaling;

import com.hazelcast.core.IMap;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.benchmarks.util.SineCurveLoadProfiler;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.NOAADataIngester;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Thilina Buddhika
 */
public class ThrottledStreamIngester extends NOAADataIngester {

    private Logger logger = Logger.getLogger(ThrottledStreamIngester.class);

    private final SineCurveLoadProfiler loadProfiler;
    private AtomicLong counter = new AtomicLong(0);
    private long tsLastEmitted = -1;
    private BufferedWriter bufferedWriter;

    public ThrottledStreamIngester() {
        super();
        loadProfiler = new SineCurveLoadProfiler(2000);
        try {
            bufferedWriter = new BufferedWriter(new FileWriter("/tmp/throughput-profile.stat"));
        } catch (IOException e) {
            logger.error("Error opening stat file for writing.", e);
        }
    }

    @Override
    public void emit() throws StreamingDatasetException {
        GeoHashIndexedRecord record = nextRecord();
        if (record != null) {
            writeToStream(Constants.Streams.NOAA_DATA_STREAM, record);
            countEmitted++;
            long sentCount = counter.incrementAndGet();
            long now = System.currentTimeMillis();

            if(tsLastEmitted == -1){
                tsLastEmitted = now;
                // register a listener for scaling in and out
                registerListener();
            } else if (now - tsLastEmitted > 1000){
                try {
                    bufferedWriter.write(now + "," + sentCount * 1000.0/(now - tsLastEmitted) + "\n");
                    bufferedWriter.flush();
                    tsLastEmitted = now;
                    counter.set(0);
                } catch (IOException e) {
                    logger.error("Error writing stats.", e);
                }
            }
            try {
                Thread.sleep(loadProfiler.nextSleepInterval());
            } catch (InterruptedException ignore) {

            }
        }
    }

    private void registerListener(){
        try {
            IMap map = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance().getMap(
                    GeoHashPrefixTree.PREFIX_MAP);
            map.addEntryListener(new DynamicScalingMonitor(DynamicScalingGraph.INITIAL_PROCESSOR_COUNT), true);
        } catch (HazelcastException e) {
            e.printStackTrace();
        }
    }
}
