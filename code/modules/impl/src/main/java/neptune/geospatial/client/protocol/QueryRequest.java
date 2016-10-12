package neptune.geospatial.client.protocol;

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
public class QueryRequest extends ControlMessage {

    private int queryId;
    private String clientUrl;
    private byte[] query;
    private List<String> geoHashes;

    public QueryRequest() {
        super(ProtocolTypes.CLIENT_QUERY_REQ);
    }

    public QueryRequest(int queryId, String clientUrl, byte[] query, List<String> geoHashes) {
        super(ProtocolTypes.CLIENT_QUERY_REQ);
        this.queryId = queryId;
        this.clientUrl = clientUrl;
        this.query = query;
        this.geoHashes = geoHashes;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.queryId = dataInputStream.readInt();
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
        dataOutputStream.writeInt(this.queryId);
        dataOutputStream.writeUTF(this.clientUrl);
        dataOutputStream.writeInt(this.query.length);
        dataOutputStream.write(this.query);
        dataOutputStream.writeInt(this.geoHashes.size());
        for (String geoHash : this.geoHashes) {
            dataOutputStream.writeUTF(geoHash);
        }
    }

    public int getQueryId() {
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
