package synopsis.client.query;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class PrefixMemUsageQueryCallback implements QueryCallback {

    private Logger logger = Logger.getLogger(PrefixMemUsageQueryCallback.class);
    private Map<String, Double> memUsageMap = new HashMap<>();
    private final int requestCount;
    private int completedRequestCount = 0;
    private int zeroResponseCount = 0;

    public PrefixMemUsageQueryCallback(int requestCount) {
        this.requestCount = requestCount;
    }

    @Override
    public synchronized void processQueryResponse(QueryResponse response) {
        String prefix = new String(response.getQuery());
        double memUsage = 0.0;
        List<byte[]> responses = response.getQueryResponse();
        for (byte[] resp : responses) {
            if (resp.length > 0) {
                memUsage += ByteBuffer.wrap(resp).getDouble();
            }
        }
        if(response.getExpectedQueryResponseCount() == 0){
            zeroResponseCount++;
        }
        memUsageMap.put(prefix, memUsage);
        completedRequestCount++;
        if(completedRequestCount == requestCount){
            logger.info("Received all responses. Dumping to the file. Zero Response Count: " + zeroResponseCount);
            dumpMemUsageToFile();
        } else {
            logger.info("Processed " + completedRequestCount + " responses. Remaining: " +
                    (requestCount - completedRequestCount) + ", zero response count: " + zeroResponseCount);
        }
    }

    private void dumpMemUsageToFile(){
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/prefix-mem-usage.csv"));
            for (String prefix : memUsageMap.keySet()){
                bufferedWriter.write(prefix + "," + memUsageMap.get(prefix) + "\n");
            }
            bufferedWriter.flush();
            bufferedWriter.close();
            logger.info("Dumped mem. usage data to file. Number of records written: " + memUsageMap.size());
        } catch (IOException e) {
            logger.error("Error writing mem. usage data to a file.", e);
        }
    }
}
