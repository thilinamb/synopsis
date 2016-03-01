package neptune.geospatial.core.deployer;

/**
 * Object representing a JSON configuration used to
 * provide a deployer config.
 *
 * @author Thilina Buddhika
 */

public class DeployerConfig {
    /**
     * Placement info for a given computation
     */
    class Placement {
        private String operator;
        private String[] nodes;

        public Placement() {
        }
    }

    private Placement[] placements;

    public DeployerConfig() {

    }
}
