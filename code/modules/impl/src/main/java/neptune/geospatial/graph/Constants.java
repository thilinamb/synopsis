package neptune.geospatial.graph;

/**
 * Constants used in the stream processing graph.
 *
 * @author Thilina Buddhika
 */
public class Constants {
    /**
     * Stream identifier definitions
     */
    public class Streams {
        public static final String GEO_HASH_INDEXED_RECORDS = "geo-hash-indexed-records";
        public static final String SPATIAL_INDEXED_RECORDS = "spatial-indexed-records";
        public static final String NOAA_DATA_STREAM = "noaa-data-stream";
        public static final String STATE_REPLICA_STREAM = "state-replica-stream";
    }
}
