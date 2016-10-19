package synopsis.client.persistence;

import neptune.geospatial.core.protocol.msg.client.PersistStateAck;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class OutstandingPersistenceTask {

    private final long persistenceTaskId;
    private final int nodeCount;
    private int ackCount;
    private int totalComputationCount;
    private List<PersistStateResponse> responses;
    private byte[] serializedPrefixTree;

    OutstandingPersistenceTask(long persistenceTaskId, int nodeCount) {
        this.persistenceTaskId = persistenceTaskId;
        this.nodeCount = nodeCount;
        this.responses = new ArrayList<>();
    }

    synchronized boolean handlePersistStateAck(PersistStateAck ack) {
        ackCount++;
        totalComputationCount += ack.getComputationCount();
        return isPersistenceTaskComplete();
    }

    synchronized boolean handlePersistStateResp(PersistStateResponse resp) {
        responses.add(resp);
        if(resp.isContainsPrefixTree()){
            this.serializedPrefixTree = resp.getPrefixTree();
        }
        return isPersistenceTaskComplete();
    }

    public long getPersistenceTaskId() {
        return persistenceTaskId;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public List<PersistStateResponse> getResponses() {
        return responses;
    }

    public int getTotalComputationCount() {
        return totalComputationCount;
    }

    public byte[] getSerializedPrefixTree() {
        return serializedPrefixTree;
    }

    private boolean isPersistenceTaskComplete() {
        if (ackCount == nodeCount) {
            if (responses.size() == totalComputationCount) {
                return true;
            }
        }
        return false;
    }
}
