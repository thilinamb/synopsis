package synopsis.client.query;

import neptune.geospatial.graph.operators.QueryCreator;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Used to track response times and payload sizes of query results for different query types
 * This class is not thread safe.
 *
 * @author Thilina Buddhika
 */
public class QClientStatRecorder {

    /**
     * Records the response times
     */
    private class QueryPerf {
        private long sum;
        private long sumOfSquares;
        private long count;

        private void update(long val) {
            this.sum += val;
            this.sumOfSquares += (val * val);
            this.count++;
        }

        private void merge(QueryPerf other) {
            this.sum += other.sum;
            this.sumOfSquares += other.sumOfSquares;
            this.count += other.count;
        }
    }

    /**
     * Records the payload sizes of the query responses
     */
    private class ResponseSizeRecorder {
        private double sum;
        private double sumOfSquares;
        private double count;

        private void update(double val) {
            this.sum += val;
            this.sumOfSquares += (val * val);
            this.count++;
        }

        private void merge(ResponseSizeRecorder other) {
            this.count += other.count;
            this.sum += other.sum;
            this.sumOfSquares += other.sumOfSquares;
        }
    }

    private Logger logger = Logger.getLogger(QClientStatRecorder.class);
    private Map<QueryCreator.QueryType, QueryPerf> performanceMap = new ConcurrentHashMap<>();
    private Map<QueryCreator.QueryType, ResponseSizeRecorder> respSizeRecMap = new ConcurrentHashMap<>();

    void record(QueryCreator.QueryType queryType, long timeElapsed, double payloadSizeInMB) {
        QueryPerf queryPerf;
        ResponseSizeRecorder responseSizeRecorder;
        if (!performanceMap.containsKey(queryType)) {
            queryPerf = new QueryPerf();
            performanceMap.put(queryType, queryPerf);
            responseSizeRecorder = new ResponseSizeRecorder();
            respSizeRecMap.put(queryType, responseSizeRecorder);
        } else {
            queryPerf = performanceMap.get(queryType);
            responseSizeRecorder = respSizeRecMap.get(queryType);
        }
        queryPerf.update(timeElapsed);
        responseSizeRecorder.update(payloadSizeInMB);
    }

    void writeToFile(String fileName) {
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            fos = new FileOutputStream(fileName);
            dos = new DataOutputStream(fos);
            for (QueryCreator.QueryType queryType : performanceMap.keySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(queryType).append(",");
                // write query perf. metrics
                QueryPerf queryPerf = performanceMap.get(queryType);
                stringBuilder.append(queryPerf.sum).append(",").append(queryPerf.sumOfSquares).append(",").append(
                        queryPerf.count);
                stringBuilder.append(",");
                // write resp. size metrics
                ResponseSizeRecorder respSizeRec = respSizeRecMap.get(queryType);
                stringBuilder.append(respSizeRec.sum).append(",").append(respSizeRec.sumOfSquares).append(",").append(respSizeRec.count);
                stringBuilder.append("\n");
                dos.writeUTF(stringBuilder.toString());
            }
            dos.flush();
            fos.flush();
        } catch (IOException e) {
            logger.error("Error writing metrics to a file. ", e);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
                if (dos != null) {
                    dos.close();
                }
            } catch (IOException e) {
                logger.error("Error closing file streams.", e);
            }
        }
    }

    void merge(QClientStatRecorder recorder) {
        for (QueryCreator.QueryType queryType : recorder.performanceMap.keySet()) {
            if (this.performanceMap.containsKey(queryType)) {
                QueryPerf perf = this.performanceMap.get(queryType);
                QueryPerf otherPerf = recorder.performanceMap.get(queryType);
                perf.merge(otherPerf);
                // merge response sizes
                ResponseSizeRecorder respSizeRec = this.respSizeRecMap.get(queryType);
                ResponseSizeRecorder otherRespSizeRec = recorder.respSizeRecMap.get(queryType);
                respSizeRec.merge(otherRespSizeRec);
            } else {
                this.performanceMap.put(queryType, recorder.performanceMap.get(queryType));
                this.respSizeRecMap.put(queryType, recorder.respSizeRecMap.get(queryType));
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(QueryCreator.QueryType.Metadata.toString());
    }
}
