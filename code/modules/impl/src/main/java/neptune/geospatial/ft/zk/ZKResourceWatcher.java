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
class ZKResourceWatcher implements Watcher {

    private final MembershipTracker membershipTracker;
    private final Logger logger = Logger.getLogger(ZKResourceWatcher.class);

    ZKResourceWatcher(MembershipTracker membershipTracker) {
        this.membershipTracker = membershipTracker;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            if(logger.isDebugEnabled()){
                logger.debug("Cluster membership has changed for path: " + watchedEvent.getPath());
            }
            if (watchedEvent.getPath().equals(Constants.ZK_ZNODE_GROUP)) {
                membershipTracker.getAvailableWorkers();
            }
        }
    }
}
