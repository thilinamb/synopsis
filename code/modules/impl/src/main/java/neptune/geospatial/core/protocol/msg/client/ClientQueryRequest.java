package neptune.geospatial.core.protocol.msg.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sent from client to an arbitrary Synopsis node
 *
 * @author Thilina Buddhika
 */
public class ClientQueryRequest extends ControlMessage {

    private long queryId;
    private String clientUrl;
    private byte[] query;
    private List<String> geoHashes;

    public ClientQueryRequest() {
        super(ProtocolTypes.CLIENT_QUERY_REQ);
    }

    public ClientQueryRequest(long queryId, String clientUrl, byte[] query, List<String> geoHashes) {
        super(ProtocolTypes.CLIENT_QUERY_REQ);
        this.queryId = queryId;
        this.clientUrl = clientUrl;
        this.query = query;
        this.geoHashes = geoHashes;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.queryId = dataInputStream.readLong();
        this.clientUrl = dataInputStream.readUTF();
        int querySize = dataInputStream.readInt();
        this.query = new byte[querySize];
        dataInputStream.readFully(this.query);
        int geoHashCount = dataInputStream.readInt();
        this.geoHashes = new ArrayList<>();
        for (int i = 0; i < geoHashCount; i++) {
            this.geoHashes.add(dataInputStream.readUTF());
        }
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(this.queryId);
        dataOutputStream.writeUTF(this.clientUrl);
        dataOutputStream.writeInt(this.query.length);
        dataOutputStream.write(this.query);
        dataOutputStream.writeInt(this.geoHashes.size());
        for (String geoHash : this.geoHashes) {
            dataOutputStream.writeUTF(geoHash);
        }
    }

    public long getQueryId() {
        return queryId;
    }

    public String getClientUrl() {
        return clientUrl;
    }

    public byte[] getQuery() {
        return query;
    }

    public List<String> getGeoHashes() {
        return geoHashes;
    }
}
