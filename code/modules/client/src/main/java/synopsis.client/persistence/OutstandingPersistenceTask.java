package synopsis.client.persistence;

import neptune.geospatial.core.protocol.msg.client.PersistStateAck;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class OutstandingPersistenceTask {

    private Logger logger = Logger.getLogger(OutstandingPersistenceTask.class);
    private boolean success;
    private long persistenceTaskId;
    private int nodeCount;
    private int ackCount;
    private int totalComputationCount;
    private Map<String, String> computationLocations;
    private Map<String, String> storageLocations;
    private byte[] serializedPrefixTree;

    public OutstandingPersistenceTask() {
        this.success = true;
        this.computationLocations = new HashMap<>();
        this.storageLocations = new HashMap<>();
    }

    OutstandingPersistenceTask(long persistenceTaskId, int nodeCount) {
        this.success = true;
        this.persistenceTaskId = persistenceTaskId;
        this.nodeCount = nodeCount;
        this.computationLocations = new HashMap<>();
        this.storageLocations = new HashMap<>();
    }

    synchronized boolean handlePersistStateAck(PersistStateAck ack) {
        logger.info("PersistenceAck from " + ack.getOriginEndpoint() + ", Computation count: " + ack.getComputationCount());
        ackCount++;
        totalComputationCount += ack.getComputationCount();
        return isPersistenceTaskComplete();
    }

synchronized boolean handlePersistStateResp(PersistStateResponse resp) {
        this.success = this.success & resp.isSuccess();
        computationLocations.put(resp.getComputationId(), resp.getOriginEndpoint());
        storageLocations.put(resp.getComputationId(), resp.getStorageLocation());
        if(resp.isContainsPrefixTree() && this.serializedPrefixTree == null){
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

    public byte[] getSerializedPrefixTree() {
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

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeBoolean(this.success);
        dos.writeLong(this.getPersistenceTaskId());
        dos.writeInt(this.nodeCount);
        dos.writeInt(this.ackCount);
        dos.writeInt(this.totalComputationCount);
        // computationLocations
        dos.writeInt(this.computationLocations.size());
        for(String key: this.computationLocations.keySet()){
            dos.writeUTF(key);
            dos.writeUTF(this.computationLocations.get(key));
        }
        // storageLocations
        dos.writeInt(this.storageLocations.size());
        for(String key: this.storageLocations.keySet()){
            dos.writeUTF(key);
            dos.writeUTF(this.storageLocations.get(key));
        }
        // serialized prefix tree
        dos.writeInt(this.serializedPrefixTree.length);
        dos.write(this.serializedPrefixTree);
    }

    public void deserialize(DataInputStream dis) throws IOException{
        this.success = dis.readBoolean();
        this.persistenceTaskId = dis.readLong();
        this.nodeCount = dis.readInt();
        this.ackCount = dis.readInt();
        this.totalComputationCount = dis.readInt();

        int compLocationCount = dis.readInt();
        for(int i = 0; i < compLocationCount; i++){
            this.computationLocations.put(dis.readUTF(), dis.readUTF());
        }

        int storageLocCount = dis.readInt();
        for(int i = 0; i < storageLocCount; i++){
            this.storageLocations.put(dis.readUTF(), dis.readUTF());
        }

        int bytesCount = dis.readInt();
        this.serializedPrefixTree = new byte[bytesCount];
        dis.readFully(this.serializedPrefixTree);
    }
}
