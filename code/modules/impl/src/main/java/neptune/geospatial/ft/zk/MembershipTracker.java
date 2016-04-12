package neptune.geospatial.ft.zk;

/**
 * @author Thilina Buddhika
 */

import ds.granules.communication.direct.ZooKeeperAgent;
import ds.granules.exception.CommunicationsException;
import ds.granules.util.Constants;
import ds.granules.util.ZooKeeperUtils;
import neptune.geospatial.ft.FTException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Keeps a current list of cluster members and notify the listeners about membership changes.
 * Currently it only notifies about members who had left the cluster.
 * <p>
 * Implemented as a singleton for each Resource. It is expensive to maintain zk clients for every computation.
 *
 * @author Thilina Buddhika
 */
public class MembershipTracker implements AsyncCallback.ChildrenCallback {

    private static MembershipTracker instance;

    private Logger logger = Logger.getLogger(MembershipTracker.class);
    private final ZooKeeper zk;
    private ZKResourceWatcher watcher;
    private List<String> members;
    private List<MembershipChangeListener> listeners = new ArrayList<>();

    private MembershipTracker() throws CommunicationsException {
        zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
        getAvailableWorkers();
    }

    public static MembershipTracker getInstance() throws CommunicationsException {
        if (instance == null) {
            synchronized (MembershipTracker.class) {
                if (instance == null) {
                    instance = new MembershipTracker();
                }
            }
        }
        return instance;
    }

    public void registerListener(MembershipChangeListener listener) {
        listeners.add(listener);
    }

    void getAvailableWorkers() {
        if (watcher == null) {
            watcher = new ZKResourceWatcher(this);
        }
        zk.getChildren(Constants.ZK_ZNODE_GROUP, watcher, this, null);
    }

    private synchronized void processClusterChanges(List<String> currentChildren) throws FTException {
        // very first invocation
        if (members == null) {
            members = new ArrayList<>();
            if (logger.isDebugEnabled()) {
                logger.debug("Started populating initial membership list...");
            }
            for (String child : currentChildren) {
                String endpoint = extractResourceEP(child);
                addMember(endpoint);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("New member discovered. Endpoint: %s, Id: %s", endpoint, child));
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Finished populating initial membership list...");
            }
        } else {
            // membership has changed. One or more processes have left the cluster
            // we should find the processors who have left the cluster
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Membership has changed. Previous count: %d, Current count: %d",
                        members.size(), currentChildren.size()));
            }
            List<String> lostProcesses = members;
            members = new ArrayList<>();
            for (String child : currentChildren) {
                String member = extractResourceEP(child);
                addMember(member);
                if (lostProcesses.contains(member)) {
                    lostProcesses.remove(member);
                }
            }
            if (lostProcesses.size() > 0 && listeners.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Notifying membership listeners. Registered listener count: %d, " +
                            "Lost member count: %d", listeners.size(), lostProcesses.size()));
                }
                // notify listeners
                for (MembershipChangeListener listener : listeners) {
                    listener.membershipChanged(Collections.unmodifiableList(lostProcesses));
                }
            }
        }
    }

    @Override
    public void processResult(int rc, String path, Object o, List<String> childNodes) {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getAvailableWorkers();
                break;
            case OK:
                try {
                    processClusterChanges(childNodes);
                } catch (FTException e) {
                    logger.error(e.getMessage(), e);
                    getAvailableWorkers();
                }
                break;
            default:
                logger.error("Error fetching child nodes.", KeeperException.create(KeeperException.Code.get(rc), path));
        }
    }

    private String extractResourceEP(String node) throws FTException {
        try {
            byte[] bytes = ZooKeeperUtils.readZNodeData(zk, Constants.ZK_ZNODE_GROUP + "/" + node);
            if (bytes != null) {
                String endPointData = new String(bytes);
                return endPointData.substring(0, endPointData.lastIndexOf(":"));
            }
        } catch (KeeperException | InterruptedException e) {
            throw new FTException("Error reading child node " + node, e);
        }
        return null;
    }

    private void addMember(String member) {
        if (member != null && !members.contains(member)) {
            members.add(member);
        }
    }
}
