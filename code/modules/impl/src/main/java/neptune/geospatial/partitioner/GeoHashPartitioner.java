package neptune.geospatial.partitioner;

import ds.funnel.topic.Topic;
import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.partition.Partitioner;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Geo-Hash based partitioner
 *
 * @author Thilina Buddhika
 */
public class GeoHashPartitioner implements Partitioner {

    private Logger logger = Logger.getLogger(GeoHashPartitioner.class);
    private List<Character> northAmericaPrefixList = new ArrayList<>();
    private boolean firstOutgoingMessage = true;
    private ShortCircuitedRoutingRegistry shortCircuitedRoutingRegistry;
    private Map<String, Integer> allLength2Prefixes = new HashMap<>();

    public GeoHashPartitioner() {
        char[] northAmericaPrefixes = new char[]{'b', 'c', '8', 'd', 'f', '9'};
        for (Character c : northAmericaPrefixes) {
            northAmericaPrefixList.add(c);
        }
        initLength2PrefixList();
    }

    @Override
    public Topic[] partition(StreamEvent streamEvent, Topic[] topics) {
        synchronized (this) {
            if (firstOutgoingMessage) {
                shortCircuitedRoutingRegistry = ShortCircuitedRoutingRegistry.getInstance();
                shortCircuitedRoutingRegistry.registerGeoHashPartitioner(this);
                firstOutgoingMessage = false;
            }
        }
        GeoHashIndexedRecord ghIndexedRec = (GeoHashIndexedRecord) streamEvent;
        if (ghIndexedRec.getHeader() == Constants.RecordHeaders.PAYLOAD) {
            // if it is a checkpointing record, then send it to all topics
            if (ghIndexedRec.getCheckpointId() > -1) {
                return topics;
            } else {
                return getReceiverTopics(topics, ghIndexedRec);
            }
        } else if (ghIndexedRec.getHeader() == Constants.RecordHeaders.PREFIX_ONLY) {
            return getReceiverTopics(topics, ghIndexedRec);
        } else {
            return topics;
        }
    }

    private Topic[] getReceiverTopics(Topic[] topics, GeoHashIndexedRecord ghIndexedRec) {
        Topic shortCircuitedTopic = shortCircuitedRoutingRegistry.getShortCircuitedRoutingRule(ghIndexedRec);
        if (shortCircuitedTopic == null) {
            if (ghIndexedRec.getPrefixLength() > 2) {  // if this is a stream processor
                int prefixLen = ghIndexedRec.getPrefixLength() * GeoHash.BITS_PER_CHAR;
                // convert the geo-hash string into the corresponding bit string
                ArrayList<Boolean> hashInBits = GeoHash.getBits(ghIndexedRec.getGeoHash());
                int sum = 0;
                for (int i = prefixLen - 1; i >= 0; i--) {
                    sum += Math.pow(2, prefixLen - 1 - i) * (hashInBits.get(prefixLen - 1 - i) ? 1 : 0);
                }
                Topic topic = topics[sum % topics.length];
                return new Topic[]{topic};
            } else {    // If it is an ingester
                String prefix = ghIndexedRec.getGeoHash().substring(0, 2);
                if (!allLength2Prefixes.containsKey(prefix)) {
                    allLength2Prefixes.put(prefix, allLength2Prefixes.size());
                    logger.info("New prefix added to the length 2 prefix map: " + prefix);
                }
                Topic topic = topics[allLength2Prefixes.get(prefix) % topics.length];
                return new Topic[]{topic};
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Found a short circuited stream. Geohash: " + ghIndexedRec.getGeoHash());
            }
            return new Topic[]{shortCircuitedTopic};
        }
    }

    private int isNorthAmerica(String geoHash) {
        return northAmericaPrefixList.indexOf(geoHash.toLowerCase().charAt(0));
    }

    private void initLength2PrefixList() {
        String[] prefixes = new String[]{"dd", "de", "dh", "dj", "dk", "dm", "f0", "dn", "f1", "f2", "dp", "f3", "dq", "dr", "f4", "ds", "f6", "dt",
                "f8", "dw", "f9", "dx", "dz", "b8", "b9", "94", "95", "96", "97", "c0", "c1", "8g", "c2", "c3", "c4", "c6", "c8", "c9", "fb", "8u", "fc",
                "8v", "fd", "bb", "8x", "ff", "bc", "8y", "8z", "bf", "9d", "9e", "9g", "9h", "d4", "d5", "9j", "d6", "9k", "d7", "9m", "9n", "9p", "9q",
                "9r", "9s", "9t", "9u", "9v", "9w", "cb", "9x", "cc", "9y", "cd", "9z", "cf"};
        for (int i = 0; i < prefixes.length; i++) {
            allLength2Prefixes.put(prefixes[i], i);
        }
    }
}
