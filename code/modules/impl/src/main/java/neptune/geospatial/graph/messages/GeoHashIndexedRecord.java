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
    private long tsIngested;

    public GeoHashIndexedRecord() {
    }

    public GeoHashIndexedRecord(String geoHash, long tsIngested) {
        this.geoHash = geoHash;
        this.tsIngested = tsIngested;
    }

    @Override
    protected void readValues(DataInputStream dataInputStream) throws IOException {
        this.geoHash = dataInputStream.readUTF();
        this.tsIngested = dataInputStream.readLong();
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.geoHash);
        dataOutputStream.writeLong(this.tsIngested);
    }

    public String getGeoHash() {
        return geoHash;
    }

    public long getTsIngested() {
        return tsIngested;
    }
}
