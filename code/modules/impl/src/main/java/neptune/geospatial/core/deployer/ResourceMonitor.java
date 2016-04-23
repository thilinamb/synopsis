package neptune.geospatial.core.deployer;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import ds.granules.communication.direct.JobDeployer;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Thilina Buddhika
 */
class ResourceMonitor implements EntryAddedListener<String, Double>, EntryUpdatedListener<String, Double> {

    private class ComparableResource implements Comparable<ComparableResource> {
        private JobDeployer.ResourceEndpoint resourceEndpoint;
        private double availableMem;

        private ComparableResource(JobDeployer.ResourceEndpoint resourceEndpoint, double availableMem) {
            this.resourceEndpoint = resourceEndpoint;
            this.availableMem = availableMem;
        }

        @Override
        public int compareTo(ComparableResource o) {
            return new Double(this.availableMem).compareTo(o.availableMem);
        }
    }

    private Map<String, ComparableResource> resources = new HashMap<>();
    private Logger logger = Logger.getLogger(ResourceMonitor.class);

    ResourceMonitor(List<JobDeployer.ResourceEndpoint> resourceEndpoints) {
        for (JobDeployer.ResourceEndpoint ep : resourceEndpoints) {
            resources.put(ep.getDataEndpoint(), new ComparableResource(ep, Double.MAX_VALUE));
        }
    }

    void init() throws HazelcastException {
        IMap<String, Double> memUsageMap = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance().
                getMap(Constants.MEMORY_USAGE_MAP);
        memUsageMap.addEntryListener(this, true);
        synchronized (this) {
            for (String endpoint : memUsageMap.keySet()) {
                resources.get(endpoint).availableMem = memUsageMap.get(endpoint);
            }
        }
    }

    @Override
    public void entryAdded(EntryEvent<String, Double> entryEvent) {
        if (resources.containsKey(entryEvent.getKey())) {
            update(entryEvent);
        }
    }

    @Override
    public void entryUpdated(EntryEvent<String, Double> entryEvent) {
        if (resources.containsKey(entryEvent.getKey())) {
            update(entryEvent);
        }
    }

    synchronized JobDeployer.ResourceEndpoint assignResource(double requiredMem, String currentLoc) {
        List<ComparableResource> resourceList = new ArrayList<>();
        resourceList.addAll(resources.values());
        Collections.sort(resourceList);
        logger.info("-------------------- sorted list -------------------");
        for (ComparableResource resource : resourceList) {
            System.out.println(resource.resourceEndpoint.getDataEndpoint() + " --> " + resource.availableMem);
        }
        logger.info("----------------------------------------------------");
        // worst case: assign the node with the highest available memory
        ComparableResource chosen = resourceList.get(resourceList.size() - 1);
        // try to find the resource with minimum available memory that can accomodate the requirement
        if (requiredMem > 0) {
            for (ComparableResource resource : resourceList) {
                if (resource.availableMem >= requiredMem && !resource.resourceEndpoint.getControlEndpoint().equals(currentLoc)) {
                    chosen = resource;
                    break;
                }
            }
        }
        logger.info(String.format("Placement request. Required mem: %.3f Assigned Mem: %.3f Endpoint:%s", requiredMem,
                chosen.availableMem, chosen.resourceEndpoint.getDataEndpoint()));
        return chosen.resourceEndpoint;
    }

    private synchronized void update(EntryEvent<String, Double> entryEvent) {
        if (resources.containsKey(entryEvent.getKey())) {
            resources.get(entryEvent.getKey()).availableMem = entryEvent.getValue();
            logger.info("Mem. Availability update: " + entryEvent.getKey() + " --> " + entryEvent.getValue() + ":" +
                    resources.get(entryEvent.getKey()).availableMem);
        } else {
            logger.warn("Invalid resource endpoint: " + entryEvent.getKey());
        }
    }
}
