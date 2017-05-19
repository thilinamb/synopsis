package neptune.geospatial.benchmarks.sketch;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.Serializer;
import neptune.geospatial.graph.operators.NOAADataIngester;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Thilina Buddhika
 */
public class ThrottledStreamIngester extends NOAADataIngester {

    private Logger logger = Logger.getLogger(ThrottledStreamIngester.class);

    private AtomicLong counter = new AtomicLong(0);
    private long tsLastEmitted = -1;
    private BufferedWriter bufferedWriter;
    private List<String> approvedGeoHashes = new ArrayList<>();

    public ThrottledStreamIngester() {
        //super(true);
        super();
        /*try {
            bufferedWriter = new BufferedWriter(new FileWriter("/tmp/throughput-profile.stat"));
        } catch (IOException e) {
            logger.error("Error opening stat file for writing.", e);
        }*/
        approvedGeoHashes.add("DN");
        approvedGeoHashes.add("DQ");
        approvedGeoHashes.add("DJ");
        approvedGeoHashes.add("DM");
    }

    @Override
    public void onSuccessfulEmission() {
        /*if (tsLastEmitted == -1) {
            try {
                Thread.sleep(20 * 1000);
                logger.debug("Initial sleep period is over. Starting to emit messages.");
            } catch (InterruptedException ignore) {

            }
        }
        long sentCount = counter.incrementAndGet();
        long now = System.currentTimeMillis();

        if (tsLastEmitted == -1) {
            tsLastEmitted = now;
        } else if (now - tsLastEmitted > 3000) {
            try {
                bufferedWriter.write(now + "," + sentCount * 1000.0 / (now - tsLastEmitted) + "\n");
                bufferedWriter.flush();
                tsLastEmitted = now;
                counter.set(0);
            } catch (IOException e) {
                logger.error("Error writing stats.", e);
            }
        }
        busywait();
        */
    }

    private void busywait() {
        for (int i = 0; i < 10; i++) {
            BigInteger bigInt = new BigInteger(2048, new Random());
            BigInteger bigInt2 = new BigInteger(512, new Random());
            bigInt.divide(bigInt2);
        }
    }

    /**
     * Extracting records only for DM, DN, DJ, DQ for months of April - July
     * @param geohash
     * @param payload
     * @return
     */
    @Override
    protected boolean filter(String geohash, byte[] payload) {
        if(!approvedGeoHashes.contains(geohash.substring(0,2).toUpperCase())){
            return false;
        }
        try {
            Metadata metadata = Serializer.deserialize(Metadata.class, payload);
            DateTime dateTime = new DateTime(metadata.getTemporalProperties().getStart());
            int month = dateTime.monthOfYear().get();
            if(month >= 4 && month <= 7){
                return true;
            }
        } catch (IOException | SerializationException e) {
            logger.error("Error deserializing binary payload.", e);
        }
        return false;
    }

    public static void main(String[] args) {
        DateTime dateTime = new DateTime(System.currentTimeMillis());
        System.out.println(dateTime.monthOfYear().get());
    }
}
