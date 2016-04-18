package neptune.geospatial.graph.messages;

import ds.granules.streaming.core.datatype.AbstractStreamEvent;

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

    /** Raw payload of this record, containing feature metadata. */
    private byte[] payload;

    private long checkpointId = -1;

    public GeoHashIndexedRecord() {
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
        this.geoHash = dataInputStream.readUTF();
        this.prefixLength = dataInputStream.readInt();
        this.messageIdentifier = dataInputStream.readLong();
        this.tsIngested = dataInputStream.readLong();
        this.payload = new byte[dataInputStream.readInt()];
        dataInputStream.readFully(this.payload);
        this.checkpointId = dataInputStream.readLong();
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.geoHash);
        dataOutputStream.writeInt(this.prefixLength);
        dataOutputStream.writeLong(this.messageIdentifier);
        dataOutputStream.writeLong(this.tsIngested);
        dataOutputStream.writeInt(this.payload.length);
        dataOutputStream.write(this.payload);
        dataOutputStream.writeLong(this.checkpointId);
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
}
