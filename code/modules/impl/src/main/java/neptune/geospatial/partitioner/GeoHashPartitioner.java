package neptune.geospatial.partitioner;

import ds.funnel.topic.Topic;
import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.partition.Partitioner;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

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

    public GeoHashPartitioner() {
        char[] northAmericaPrefixes = new char[]{'b', 'c', '8', 'd', 'f', '9'};
        for (Character c : northAmericaPrefixes) {
            northAmericaPrefixList.add(c);
        }
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
            int prefixLen = ghIndexedRec.getPrefixLength() * GeoHash.BITS_PER_CHAR;
            // convert the geo-hash string into the corresponding bit string
            ArrayList<Boolean> hashInBits = GeoHash.getBits(ghIndexedRec.getGeoHash());
            int sum = 0;
            for (int i = prefixLen - 1; i >= 0; i--) {
                sum += Math.pow(2, prefixLen - 1 - i) * (hashInBits.get(prefixLen - 1 - i) ? 1 : 0);
            }
            Topic topic = topics[sum % topics.length];
            return new Topic[]{topic};
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
}
