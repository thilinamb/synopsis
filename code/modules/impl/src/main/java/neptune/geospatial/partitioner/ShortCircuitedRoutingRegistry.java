package neptune.geospatial.partitioner;

import ds.funnel.topic.Topic;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class ShortCircuitedRoutingRegistry {

    private static ShortCircuitedRoutingRegistry instance = new ShortCircuitedRoutingRegistry();
    private GeoHashPartitioner partitioner;
    private Map<String, Topic> routingTable = new ConcurrentHashMap<>();

    private ShortCircuitedRoutingRegistry() {
        // Singleton instance: private constructor
    }

    public static ShortCircuitedRoutingRegistry getInstance() {
        return instance;
    }

    public synchronized void registerGeoHashPartitioner(GeoHashPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public synchronized GeoHashPartitioner getPartitioner() {
        return partitioner;
    }

    public void addShortCircuitedRoutingRule(String prefix, Topic topic) {
        routingTable.put(prefix, topic);
    }

    public Topic getShortCircuitedRoutingRule(GeoHashIndexedRecord record) {
        String prefix = record.getGeoHash();
        Topic topic = null;
        int longestMatchPrefixLength = 0;
        for (String shortCircuitedPrefix : routingTable.keySet()) {
            if (prefix.startsWith(shortCircuitedPrefix) && shortCircuitedPrefix.length() > longestMatchPrefixLength) {
                topic = routingTable.get(shortCircuitedPrefix);
                longestMatchPrefixLength = shortCircuitedPrefix.length();
            }
        }
        if(topic != null){
            record.setPrefixLength(longestMatchPrefixLength);
        }
        return topic;
    }
}
