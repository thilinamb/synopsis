package neptune.geospatial.graph.operators;

import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * This is a temporary operator used to count
 * the number of records in order to make sure
 * initial setup works.
 *
 * @author Thilina Buddhika
 */
public class RecordCounter extends AbstractGeoSpatialStreamProcessor {

    private Logger logger = Logger.getLogger(RecordCounter.class);
    private int counter = 0;

    @Override
    protected void process(GeoHashIndexedRecord record) {
        if (++counter % 100000 == 0) {
            logger.info(String.format("[%s] Record received. Counter: %d", getInstanceIdentifier(), counter));
        }
    }

    @Override
    public byte[] split(String prefix) {
        return new byte[0];
    }

    @Override
    public void merge(String prefix, byte[] serializedSketch) {

    }

    @Override
    public byte[] query(byte[] query) {
        throw new UnsupportedOperationException("The method 'query' is not supported");
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) {
        throw new UnsupportedOperationException("The method 'serialize' is not supported");
    }

    @Override
    public void deserialize(DataInputStream dataInputStream) {
        throw new UnsupportedOperationException("The method 'deserialize' is not supported");
    }
}
