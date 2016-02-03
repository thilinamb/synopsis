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

    public GeoHashIndexedRecord() {
    }

    public GeoHashIndexedRecord(String geoHash, int prefixLength, long tsIngested) {
        this.geoHash = geoHash;
        this.prefixLength = prefixLength;
        this.tsIngested = tsIngested;
    }

    @Override
    protected void readValues(DataInputStream dataInputStream) throws IOException {
        this.geoHash = dataInputStream.readUTF();
        this.prefixLength = dataInputStream.readInt();
        this.tsIngested = dataInputStream.readLong();
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.geoHash);
        dataOutputStream.writeInt(prefixLength);
        dataOutputStream.writeLong(this.tsIngested);
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
}
