package neptune.geospatial.core.deployer;

import java.util.HashMap;
import java.util.Map;

/**
 * Object representing a JSON configuration used to
 * provide a deployer config.
 *
 * @author Thilina Buddhika
 */
@SuppressWarnings("unused")
public class DeployerConfig {
    /**
     * Placement info for a given computation as encoded in JSON.
     */
    class Placement {
        private String operator;
        private String[] nodes;
        private int lastDeployed;

        public Placement() {
        }
    }

    private Placement[] placements;
    private Map<String, Placement> deploymentStateMap;

    public DeployerConfig() {

    }

    /**
     * Indexes the config based on the class names.
     * This is a workaround due to limited support for
     * collections in Gson API.
     */
    private void index() {
        deploymentStateMap = new HashMap<>();
        if (placements != null) {
            for (Placement placement : placements) {
                deploymentStateMap.put(placement.operator, placement);
            }
        }
    }

    public String getPlacementNode(String operatorClass) {
        if (deploymentStateMap == null) {
            index();
        }
        Placement depState = deploymentStateMap.get(operatorClass);
        if (depState != null) {
            return depState.nodes[depState.lastDeployed++];
        } else {
            return null;
        }
    }
}
