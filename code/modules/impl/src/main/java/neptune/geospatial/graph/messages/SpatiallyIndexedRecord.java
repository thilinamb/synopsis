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
public class SpatiallyIndexedRecord extends AbstractStreamEvent {

    /**
     * Geohash for the location associated with this record (in binary form).
     * Since 0 is a valid Geohash we'll default this to a negative value.
     */
    private long geoBits = -1;

    /** Raw payload of this record, containing feature metadata. */
    private byte[] payload;

    public SpatiallyIndexedRecord() {

    }

    /**
     * Constructs a new spatially indexed record.
     *
     * @param geoBits The geohash for this record, in binary form.
     * @param payload The raw {@link io.sigpipe.sing.dataset.Metadata}
     * representation of the record.
     */
    public SpatiallyIndexedRecord(long geoBits, byte[] payload) {
        this.geoBits = geoBits;
        this.payload = payload;
    }

    public long getGeoHash() {
        return this.geoBits;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    @Override
    protected void readValues(DataInputStream dataInputStream)
    throws IOException {
        this.geoBits = dataInputStream.readLong();
        int payloadSize = dataInputStream.readInt();
        this.payload = new byte[payloadSize];
        dataInputStream.read(this.payload);
    }

    @Override
    protected void writeValues(DataOutputStream dataOutputStream)
    throws IOException {
        dataOutputStream.writeLong(this.geoBits);
        dataOutputStream.writeInt(this.payload.length);
        dataOutputStream.write(this.payload);
    }
}
