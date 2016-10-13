package synopsis.client.query;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class QueryResponse {
    private long queryId;
    private byte[] query;
    private int expectedQueryResponseCount;
    private List<byte[]> queryResponse;
    private List<Long> elapsedTimesInSketchlets;
    private long elapsedTime;
    private long startTime;

    public QueryResponse(long queryId, byte[] query) {
        this.startTime = System.nanoTime();
        this.queryId = queryId;
        this.query = query;
        this.queryResponse = new ArrayList<>();
        this.elapsedTimesInSketchlets = new ArrayList<>();
    }

    public synchronized int getExpectedQueryResponseCount() {
        return expectedQueryResponseCount;
    }

    public synchronized boolean setExpectedQueryResponseCount(int expectedQueryResponseCount) {
        boolean fireCallback = false;
        this.expectedQueryResponseCount = expectedQueryResponseCount;
        if(this.expectedQueryResponseCount == this.queryResponse.size()){
            fireCallback = true;
        }
        return fireCallback;
    }

    public synchronized boolean addQueryResponse(byte[] queryResp, long elapsedTime){
        boolean fireCallback = false;
        queryResponse.add(queryResp);
        elapsedTimesInSketchlets.add(elapsedTime);
        if(this.queryResponse.size() == this.expectedQueryResponseCount) {
            fireCallback = true;
        }
        return fireCallback;
    }

    public void setElapsedTime(){
        this.elapsedTime = System.nanoTime() - startTime;
    }

    public long getQueryId() {
        return queryId;
    }

    public byte[] getQuery() {
        return query;
    }

    public List<byte[]> getQueryResponse() {
        return queryResponse;
    }

    public List<Long> getElapsedTimesInSketchlets() {
        return elapsedTimesInSketchlets;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }
}
