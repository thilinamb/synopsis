package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class TargetQueryResponse extends ControlMessage {

    private long queryId;
    private String compId;
    private byte[] response;
    private long queryEvalTime;

    public TargetQueryResponse() {
        super(ProtocolTypes.TARGET_QUERY_RESP);
    }

    public TargetQueryResponse(long queryId, String compId, byte[] response, long queryEvalTime) {
        super(ProtocolTypes.TARGET_QUERY_RESP);
        this.queryId = queryId;
        this.compId = compId;
        this.response = response;
        this.queryEvalTime = queryEvalTime;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.queryId = dataInputStream.readLong();
        this.compId = dataInputStream.readUTF();
        this.response = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.response);
        this.queryEvalTime = dataInputStream.readLong();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(this.queryId);
        dataOutputStream.writeUTF(this.compId);
        dataOutputStream.writeInt(this.response.length);
        dataOutputStream.write(this.response);
        dataOutputStream.writeLong(this.queryEvalTime);
    }

    public long getQueryId() {
        return queryId;
    }

    public String getCompId() {
        return compId;
    }

    public byte[] getResponse() {
        return response;
    }

    public long getQueryEvalTime() {
        return queryEvalTime;
    }
}
