package synopsis.client.persistence;

import neptune.geospatial.core.protocol.msg.client.PersistStateAck;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
class OutstandingPersistenceTask {

    private boolean success;
    private final long persistenceTaskId;
    private final int nodeCount;
    private int ackCount;
    private int totalComputationCount;
    private Map<String, String> computationLocations;
    private Map<String, String> storageLocations;
    private byte[] serializedPrefixTree;

    OutstandingPersistenceTask(long persistenceTaskId, int nodeCount) {
        this.success = true;
        this.persistenceTaskId = persistenceTaskId;
        this.nodeCount = nodeCount;
        this.computationLocations = new HashMap<>();
        this.storageLocations = new HashMap<>();
    }

    synchronized boolean handlePersistStateAck(PersistStateAck ack) {
        ackCount++;
        totalComputationCount += ack.getComputationCount();
        return isPersistenceTaskComplete();
    }

    synchronized boolean handlePersistStateResp(PersistStateResponse resp) {
        this.success = this.success & resp.isSuccess();
        computationLocations.put(resp.getComputationId(), resp.getOriginEndpoint());
        storageLocations.put(resp.getComputationId(), resp.getStorageLocation());
        if(resp.isContainsPrefixTree()){
            this.serializedPrefixTree = resp.getPrefixTree();
        }
        return isPersistenceTaskComplete();
    }

    long getPersistenceTaskId() {
        return persistenceTaskId;
    }

    int getNodeCount() {
        return nodeCount;
    }

    Map<String, String> getComputationLocations() {
        return computationLocations;
    }

    Map<String, String> getStorageLocations() {
        return storageLocations;
    }

    int getTotalComputationCount() {
        return totalComputationCount;
    }

    byte[] getSerializedPrefixTree() {
        return serializedPrefixTree;
    }

    private boolean isPersistenceTaskComplete() {
        if (ackCount == nodeCount) {
            if (computationLocations.size() == totalComputationCount) {
                return true;
            }
        }
        return false;
    }

    boolean isSuccess() {
        return success;
    }

    public void setSerializedPrefixTree(byte[] serializedPrefixTree) {
        this.serializedPrefixTree = serializedPrefixTree;
    }
}
