package neptune.geospatial.partitioner;

import ds.funnel.topic.Topic;
import ds.granules.dataset.StreamEvent;
import ds.granules.streaming.core.partition.Partitioner;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.util.geohash.GeoHash;

import java.util.ArrayList;

/**
 * Geo-Hash based partitioner
 *
 * @author Thilina Buddhika
 */
public class GeoHashPartitioner implements Partitioner {

    @Override
    public Topic[] partition(StreamEvent streamEvent, Topic[] topics) {
        GeoHashIndexedRecord ghIndexedRec = (GeoHashIndexedRecord) streamEvent;
        int prefix = ghIndexedRec.getPrefixLength();
        // convert the geohash string into the corresponding bit string
        ArrayList<Boolean> hashInBits = GeoHash.getBits(ghIndexedRec.getGeoHash());
        int sum = 0;
        for (int i = prefix - 1; i >= 0; i--) {
            sum += Math.pow(2, prefix - 1 - i) * (hashInBits.get(i) ? 1 : 0);
        }
        Topic topic = topics[sum % topics.length];
        return new Topic[]{topic};
    }
}
