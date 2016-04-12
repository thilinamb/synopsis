package neptune.geospatial.ft.zk;

import ds.granules.util.Constants;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watches for events related to changes in the Neptune cluster
 *
 * @author Thilina Buddhika
 */
public class ZKResourceWatcher implements Watcher {

    private final MembershipTracker edgeCache;
    private final Logger logger = Logger.getLogger(ZKResourceWatcher.class);

    public ZKResourceWatcher(MembershipTracker edgeCache) {
        this.edgeCache = edgeCache;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if(logger.isDebugEnabled()){
                logger.debug("Cluster membership has changed for path: " + watchedEvent.getPath());
            }
            if (watchedEvent.getPath().equals(Constants.ZK_ZNODE_GROUP)) {
                edgeCache.getAvailableWorkers();
            }
        }
    }
}
