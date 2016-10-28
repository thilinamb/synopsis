package neptune.geospatial.benchmarks.sketch;

import neptune.geospatial.graph.operators.NOAADataIngester;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
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

    public ThrottledStreamIngester() {
        //super(true);
        super();
        try {
            bufferedWriter = new BufferedWriter(new FileWriter("/tmp/throughput-profile.stat"));
        } catch (IOException e) {
            logger.error("Error opening stat file for writing.", e);
        }
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
}
