package neptune.geospatial.core.computations.scalingctxt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents an on-going Scaling in task.
 *
 * @author Thilina Buddhika
 */
public class PendingScaleInRequest {
    private String prefix;
    private int sentCount;
    private int receivedCount;
    private String originCtrlEndpoint;
    private String originComputation;
    private boolean initiatedLocally;
    private boolean lockAcquired = true;
    private Map<String, FullQualifiedComputationAddr> sentOutRequests = new HashMap<>();
    private List<String> locallyProcessedPrefixes = new ArrayList<>();
    private List<String> childLeafPrefixes = new ArrayList<>();

    public PendingScaleInRequest(String prefix, int sentCount, String originCtrlEndpoint, String originComputation) {
        this.prefix = prefix;
        this.sentCount = sentCount;
        this.originCtrlEndpoint = originCtrlEndpoint;
        this.originComputation = originComputation;
        this.initiatedLocally = false;
    }

    public PendingScaleInRequest(String prefix, int sentCount) {
        this.prefix = prefix;
        this.sentCount = sentCount;
        this.initiatedLocally = true;
    }

    public String getPrefix() {
        return prefix;
    }

    public void addSentOutRequest(String prefix, FullQualifiedComputationAddr targetCompAddr){
        sentOutRequests.put(prefix, targetCompAddr);
    }

    public void setLocallyProcessedPrefix(List<String> localPrefix){
        locallyProcessedPrefixes = localPrefix;
    }

    public void setSentOutRequests(Map<String, FullQualifiedComputationAddr> sentOutRequests) {
        this.sentOutRequests = sentOutRequests;
    }

    public int incrementAndGetReceivedCount(){
        return ++receivedCount;
    }

    public boolean updateAndGetLockStatus(boolean lockStatus){
        lockAcquired = lockAcquired & lockStatus;
        return lockAcquired;
    }

    public void addChildPrefixes(List<String> childPrefixes){
        childLeafPrefixes.addAll(childPrefixes);
    }

    public int getSentCount() {
        return sentCount;
    }

    public String getOriginCtrlEndpoint() {
        return originCtrlEndpoint;
    }

    public String getOriginComputation() {
        return originComputation;
    }

    public boolean isInitiatedLocally() {
        return initiatedLocally;
    }

    public Map<String, FullQualifiedComputationAddr> getSentOutRequests() {
        return sentOutRequests;
    }

    public List<String> getLocallyProcessedPrefixes() {
        return locallyProcessedPrefixes;
    }

    public List<String> getChildLeafPrefixes() {
        return childLeafPrefixes;
    }

    public void setReceivedCount(int receivedCount) {
        this.receivedCount = receivedCount;
    }
}
