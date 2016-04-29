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

    /**
     * Operators names
     */
    public class Operators {
        public static final String STATE_REPLICA_PROCESSOR_NAME = "state-replica-processor";
    }

    public class ZNodes {
        public static final String ZNODE_BACKUP_TOPICS = "/ft-backup-topics";
    }

    public static final String MEMORY_USAGE_MAP = "mem-usage-map";

    public class RecordHeaders {
        public static final int PREFIX_ONLY = 0;
        public static final int SCALE_OUT = 1;
        public static final int PAYLOAD = 2;
    }
}
