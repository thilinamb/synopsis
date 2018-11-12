package neptune.geospatial.core.deployer;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import ds.granules.communication.direct.JobDeployer;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.hazelcast.HazelcastClientInstanceHolder;
import neptune.geospatial.hazelcast.HazelcastException;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Thilina Buddhika
 */
class ResourceMonitor implements EntryAddedListener<String, Double>, EntryUpdatedListener<String, Double> {

    private class ComparableResource implements Comparable<ComparableResource> {
        private JobDeployer.ResourceEndpoint resourceEndpoint;
        private double availableMem;
        private int compCount;
        private boolean availableForScheduling;
        private int memoryUpdatesSinceLastScheduling = 0;

        private ComparableResource(JobDeployer.ResourceEndpoint resourceEndpoint, double availableMem) {
            this.resourceEndpoint = resourceEndpoint;
            this.availableMem = availableMem;
            this.compCount = 0;
            this.availableForScheduling = true;
        }

        @Override
        public int compareTo(ComparableResource o) {
            return new Double(this.availableMem).compareTo(o.availableMem);
        }
    }

    private Map<String, ComparableResource> resources = new HashMap<>();
    private Logger logger = Logger.getLogger(ResourceMonitor.class);

    ResourceMonitor(List<JobDeployer.ResourceEndpoint> resourceEndpoints, List<JobDeployer.ResourceEndpoint> layerOneEndpoints) {
        for (JobDeployer.ResourceEndpoint ep : resourceEndpoints) {
            if (!layerOneEndpoints.contains(ep)) {
                resources.put(ep.getDataEndpoint(), new ComparableResource(ep, Double.MAX_VALUE));
            } else {
                logger.info(String.format("Skipping the layer one endpoint: %s", ep.getDataEndpoint()));
            }
        }
    }

    void init() throws HazelcastException {
        IMap<String, Double> memUsageMap = HazelcastClientInstanceHolder.getInstance().getHazelcastClientInstance().
                getMap(Constants.MEMORY_USAGE_MAP);
        memUsageMap.addEntryListener(this, true);
        synchronized (this) {
            for (String endpoint : memUsageMap.keySet()) {
                if (resources.containsKey(endpoint)) {
                    resources.get(endpoint).availableMem = memUsageMap.get(endpoint);
                }
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
        // assign the node with the highest available memory
        ComparableResource chosen = null;
        for (int i = resources.size() - 1; i >= 0; i--) {
            ComparableResource resource = resourceList.get(i);
            if (resource.availableMem >= requiredMem && !resource.resourceEndpoint.getControlEndpoint().equals(currentLoc)
                    && resource.availableForScheduling) {
                resource.compCount++;
                chosen = resource;
                resource.availableForScheduling = false;
                break;
            }
        }
        //logger.info(String.format("Placement request. Required mem: %.3f Assigned Mem: %.3f Endpoint: %s", requiredMem,
        //        chosen.availableMem, chosen.resourceEndpoint.getDataEndpoint()));
        if (chosen == null) {
            logger.info("No endpoints are available for scheduling.");
            return null;
        }
        return chosen.resourceEndpoint;
    }

    private synchronized void update(EntryEvent<String, Double> entryEvent) {
        if (resources.containsKey(entryEvent.getKey())) {
            ComparableResource resource = resources.get(entryEvent.getKey());
            resource.availableMem = entryEvent.getValue();
            if (!resource.availableForScheduling) {
                resource.memoryUpdatesSinceLastScheduling++;
                if (resource.memoryUpdatesSinceLastScheduling == 2) {
                    resource.availableForScheduling = true;
                    resource.memoryUpdatesSinceLastScheduling = 0;
                }
            }
            logger.info(entryEvent.getKey() + "-> available for scheduling: " +
                    resource.availableForScheduling + ", available mem: " +
                    RivuletUtil.inGigabytes(resource.availableMem) + ", comp. count: " +
                    resource.compCount);
        } else {
            logger.warn("Invalid resource endpoint: " + entryEvent.getKey());
        }
    }
}
