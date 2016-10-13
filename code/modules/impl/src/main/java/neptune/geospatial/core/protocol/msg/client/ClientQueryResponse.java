package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ClientQueryResponse extends ControlMessage {

    private long queryId;
    private int targetCompCount;

    public ClientQueryResponse() {
        super(ProtocolTypes.CLIENT_QUERY_RESP);
    }

    public ClientQueryResponse(long queryId, int targetCompCount) {
        super(ProtocolTypes.CLIENT_QUERY_RESP);
        this.queryId = queryId;
        this.targetCompCount = targetCompCount;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.queryId = dataInputStream.readLong();
        this.targetCompCount = dataInputStream.readInt();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(this.queryId);
        dataOutputStream.writeInt(this.targetCompCount);
    }

    public long getQueryId() {
        return queryId;
    }

    public int getTargetCompCount() {
        return targetCompCount;
    }
}
