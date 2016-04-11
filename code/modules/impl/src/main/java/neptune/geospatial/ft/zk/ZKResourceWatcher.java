package neptune.geospatial.ft.zk;

import ds.granules.util.Constants;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watches for events related to changes in the Neptune cluster
 *
 * @author Thilina Buddhika
 */
public class ZKResourceWatcher implements Watcher {

    private final MembershipTracker edgeCache;

    public ZKResourceWatcher(MembershipTracker edgeCache) {
        this.edgeCache = edgeCache;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if (watchedEvent.getPath().equals(Constants.ZK_ZNODE_GROUP)) {
                edgeCache.getAvailableWorkers();
            }
        }
    }
}
