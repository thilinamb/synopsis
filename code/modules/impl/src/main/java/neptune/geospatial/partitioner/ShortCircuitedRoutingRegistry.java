package neptune.geospatial.partitioner;

import ds.funnel.topic.Topic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class ShortCircuitedRoutingRegistry {

    private static ShortCircuitedRoutingRegistry instance = new ShortCircuitedRoutingRegistry();
    private GeoHashPartitioner partitioner;
    private Map<String, Topic> routingTable = new ConcurrentHashMap<>();

    private ShortCircuitedRoutingRegistry(){
        // Singleton instance: private constructor
    }

    public static ShortCircuitedRoutingRegistry getInstance(){
        return instance;
    }

    public void registerGeoHashPartitioner(GeoHashPartitioner partitioner){
        this.partitioner = partitioner;
    }

    public GeoHashPartitioner getPartitioner(){
        return partitioner;
    }

    public void addShortCircuitedRoutingRule(String prefix, Topic topic){
        routingTable.put(prefix, topic);
    }

    public Topic getShortCircuitedRoutingRule(String prefix){
        return null;
    }
}
