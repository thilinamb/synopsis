package neptune.geospatial.core.computations.scalingctxt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Thilina Buddhika
 */
public class ScalingContext {
    private Set<MonitoredPrefix> monitoredPrefixes = new TreeSet<>();
    private Map<String, MonitoredPrefix> monitoredPrefixMap = new HashMap<>();
    private Map<String, PendingScaleOutRequest> pendingScaleOutRequests = new HashMap<>();
    private Map<String, PendingScaleInRequest> pendingScaleInRequests = new HashMap<>();
}
