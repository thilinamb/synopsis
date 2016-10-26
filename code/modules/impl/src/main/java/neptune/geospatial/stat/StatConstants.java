package neptune.geospatial.stat;

/**
 * @author Thilina Buddhika
 */
public class StatConstants {
    public class MessageTypes {
        public static final int REGISTER = 13021;
        public static final int PERIODIC_UPDATE = 13022;
        public static final int STAT_ACTIVITY = 13023;
    }

    public class ProcessorTypes {
        public static final boolean INGESTER = false;
        public static final boolean PROCESSOR = true;
    }

    public class RegistryIndices{
        // processor related metrics
        public static final int PROC_BACKLOG = 0;
        public static final int PROC_MEMORY = 1;
        public static final int PROC_LOCALLY_PROCESSED_PREF_COUNT = 2;
        public static final int PROC_THROUGHPUT = 3;
        public static final int PROC_PREFIX_LENGTH = 4;
        public static final int PROC_QUERY_THROUGHPUT = 5;
        // ingester related metrics
        public static final int ING_SENT_MSG_COUNT = 0;
        public static final int ING_SENT_BYTE_COUNT = 1;
    }

    public class ScaleActivityType{
        public static final boolean SCALE_OUT = true;
        public static final boolean SCALE_IN = false;
    }

    public class ScaleActivityEvent{
        public static final boolean START = true;
        public static final boolean END = false;
    }
}
