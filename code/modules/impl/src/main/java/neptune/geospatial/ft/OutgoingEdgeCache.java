package neptune.geospatial.ft;

import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.exception.CommunicationsException;
import ds.granules.util.Constants;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps a list of outgoing edges from the current node.
 *
 * @author Thilina Buddhika
 */
public class OutgoingEdgeCache implements AsyncCallback.ChildrenCallback {

    private Logger logger = Logger.getLogger(OutgoingEdgeCache.class);
    private final List<String> outgoingEdges;
    private final ZooKeeper zk;
    private ZKResourceWatcher watcher;

    public OutgoingEdgeCache(List<String> outgoingEdges) throws CommunicationsException {
        this.outgoingEdges = outgoingEdges;
        zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
    }

    public void nodesChanged() {
        getAvailableWorkers();
    }

    public void getAvailableWorkers() {
        if (watcher == null) {
            watcher = new ZKResourceWatcher(this);
        }
        zk.getChildren(Constants.ZK_ZNODE_GROUP, watcher, this, null);
    }

    private List<String> processClusterChanges(List<String> currentChildren){
        List<String> lostNodes = new ArrayList<>();
        for(String node : outgoingEdges){
            if(!currentChildren.contains(node)){
                lostNodes.add(node);
            }
        }
        return lostNodes;
    }

    @Override
    public void processResult(int rc, String path, Object o, List<String> childNodes) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getAvailableWorkers();
                break;
            case OK:
                processClusterChanges(childNodes);
                break;
            default:
                logger.error("Error fetching child nodes.", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    }
}
