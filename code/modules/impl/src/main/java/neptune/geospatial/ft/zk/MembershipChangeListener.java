package neptune.geospatial.ft.zk;

import java.util.List;

/**
 * Listens to membership changes in the cluster.
 *
 * @author Thilina Buddhika
 */
public interface MembershipChangeListener {

    /**
     * Membership in the cluster has changed.
     * Invoked by the {@link MembershipTracker}.
     *
     * @param lostMembers An unmodifiable list of cluster resources who have left the cluster
     */
    public void membershipChanged(List<String> lostMembers);
}
