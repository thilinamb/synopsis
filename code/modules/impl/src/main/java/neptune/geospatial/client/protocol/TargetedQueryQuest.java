package neptune.geospatial.client.protocol;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class TargetedQueryQuest extends ControlMessage {

    private int queryId;
    private byte[] query;
    private List<String> targetComputationIds;
    private String clientAddr;

    public TargetedQueryQuest() {
        super(ProtocolTypes.PROPAGATED_QUERY_REQ);
    }

    public TargetedQueryQuest(int queryId, byte[] query, List<String> targetComputationIds, String clientAddr) {
        super(ProtocolTypes.PROPAGATED_QUERY_REQ);
        this.queryId = queryId;
        this.query = query;
        this.targetComputationIds = targetComputationIds;
        this.clientAddr = clientAddr;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.queryId = dataInputStream.readInt();
        // deserialize query
        this.query = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.query);
        // deserialize computation id
        int compIdCount = dataInputStream.readInt();
        this.targetComputationIds = new ArrayList<>();
        for (int i = 0; i < compIdCount; i++) {
            targetComputationIds.add(dataInputStream.readUTF());
        }
        this.clientAddr = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.queryId);
        dataOutputStream.writeInt(this.query.length);
        dataOutputStream.write(this.query);
        dataOutputStream.writeInt(this.targetComputationIds.size());
        for (String targetComp : this.targetComputationIds) {
            dataOutputStream.writeUTF(targetComp);
        }
        dataOutputStream.writeUTF(this.clientAddr);
    }

    public int getQueryId() {
        return queryId;
    }

    public byte[] getQuery() {
        return query;
    }

    public List<String> getCompId() {
        return targetComputationIds;
    }

    public String getClientAddr() {
        return clientAddr;
    }
}
