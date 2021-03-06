package neptune.geospatial.graph.messages;

import ds.granules.streaming.core.datatype.AbstractStreamEvent;
import neptune.geospatial.graph.Constants;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A geo-hash indexed record.
 *
 * @author Thilina Buddhika
 */
public class GeoHashIndexedRecord extends AbstractStreamEvent {

    /**
     * Type of the geohash record
     */
    private int header = Constants.RecordHeaders.PAYLOAD;

    /**
     * Geohash for the record
     */
    private String geoHash;
    /**
     * Prefix length(in bits) considered by the previous emitter
     */
    private int prefixLength;

    // this is a temp field until we populate the messages with real data
    private long tsIngested;

    /**
     * Uniquely identifies each message.
     */
    private long messageIdentifier;

    /**
     * Raw payload of this record, containing feature metadata.
     */
    private byte[] payload;

    private long checkpointId = -1;

    private String parentId;

    private String parentEndpoint;

    private String ingesterId;

    private String ingesterEndpoint;

    public GeoHashIndexedRecord() {
    }

    public GeoHashIndexedRecord(int header, String ingesterId, String ingesterEndpoint) {
        this.header = header;
        this.ingesterId = ingesterId;
        this.ingesterEndpoint = ingesterEndpoint;
    }

    public GeoHashIndexedRecord(int header, String geoHash, int prefixLength) {
        this.header = header;
        this.geoHash = geoHash;
        this.prefixLength = prefixLength;
    }

    public GeoHashIndexedRecord(long checkpointId, String parentId, String parentEndpoint) {
        this.checkpointId = checkpointId;
        this.parentId = parentId;
        this.parentEndpoint = parentEndpoint;
    }

    public GeoHashIndexedRecord(String geoHash, int prefixLength, long messageIdentifier, long tsIngested, byte[] payload) {
        this.geoHash = geoHash;
        this.prefixLength = prefixLength;
        this.messageIdentifier = messageIdentifier;
        this.tsIngested = tsIngested;
        this.payload = payload;
    }

    @Override
    protected void readValues(DataInputStream dataInputStream) throws IOException {
        this.header = dataInputStream.readInt();
        if (this.header == Constants.RecordHeaders.PAYLOAD) {
            this.checkpointId = dataInputStream.readLong();
            if (this.checkpointId > -1) {
                this.parentId = dataInputStream.readUTF();
                this.parentEndpoint = dataInputStream.readUTF();
            } else {
                this.geoHash = dataInputStream.readUTF();
                this.prefixLength = dataInputStream.readInt();
                this.messageIdentifier = dataInputStream.readLong();
                this.tsIngested = dataInputStream.readLong();
                this.payload = new byte[dataInputStream.readInt()];
                dataInputStream.readFully(this.payload);
            }
        } else if (header == Constants.RecordHeaders.PREFIX_ONLY) {
            this.geoHash = dataInputStream.readUTF();
            this.prefixLength = dataInputStream.readInt();
        } else {
            this.ingesterId = dataInputStream.readUTF();
            this.ingesterEndpoint = dataInputStream.readUTF();
        }
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(header);
        if (header == Constants.RecordHeaders.PAYLOAD) {
            dataOutputStream.writeLong(this.checkpointId);
            if (this.checkpointId > -1) {
                dataOutputStream.writeUTF(this.parentId);
                dataOutputStream.writeUTF(this.parentEndpoint);
            } else {
                dataOutputStream.writeUTF(this.geoHash);
                dataOutputStream.writeInt(this.prefixLength);
                dataOutputStream.writeLong(this.messageIdentifier);
                dataOutputStream.writeLong(this.tsIngested);
                dataOutputStream.writeInt(this.payload.length);
                dataOutputStream.write(this.payload);
            }
        } else if (header == Constants.RecordHeaders.PREFIX_ONLY) {
            dataOutputStream.writeUTF(this.geoHash);
            dataOutputStream.writeInt(this.prefixLength);
        } else {
            dataOutputStream.writeUTF(this.getIngesterId());
            dataOutputStream.writeUTF(this.getIngesterEndpoint());
        }
    }

    public String getGeoHash() {
        return geoHash;
    }

    public long getTsIngested() {
        return tsIngested;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public void setPrefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    public long getMessageIdentifier() {
        return messageIdentifier;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getParentId() {
        return parentId;
    }

    public String getParentEndpoint() {
        return parentEndpoint;
    }

    public int getHeader() {
        return header;
    }

    public String getIngesterId() {
        return ingesterId;
    }

    public String getIngesterEndpoint() {
        return ingesterEndpoint;
    }
}
