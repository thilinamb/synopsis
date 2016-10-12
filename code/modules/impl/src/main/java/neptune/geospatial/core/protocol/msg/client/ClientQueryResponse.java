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

    private int queryId;
    private int targetCompCount;

    public ClientQueryResponse() {
        super(ProtocolTypes.CLIENT_QUERY_RESP);
    }

    public ClientQueryResponse(int queryId, int targetCompCount) {
        super(ProtocolTypes.CLIENT_QUERY_RESP);
        this.queryId = queryId;
        this.targetCompCount = targetCompCount;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.queryId = dataInputStream.readInt();
        this.targetCompCount = dataInputStream.readInt();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(this.queryId);
        dataOutputStream.writeInt(this.targetCompCount);
    }

    public int getQueryId() {
        return queryId;
    }

    public int getTargetCompCount() {
        return targetCompCount;
    }
}
