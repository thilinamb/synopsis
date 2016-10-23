package synopsis.client.persistence;

import ds.granules.exception.CommunicationsException;
import ds.granules.exception.DeploymentException;
import ds.granules.exception.MarshallingException;
import ds.granules.operation.Operation;
import neptune.geospatial.core.deployer.GeoSpatialDeployer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @author Thilina Buddhika
 */
public class LoadStateFromDiskDeployer extends GeoSpatialDeployer {

    private final Logger logger = Logger.getLogger(LoadStateFromDiskDeployer.class);

    private final OutstandingPersistenceTask outstandingPersistenceTask;
    private List<String> oldComputations = new ArrayList<>();
    private int completedComputationIndex = 0;
    private Map<String, ResourceEndpoint> endpointMap = new HashMap<>();

    public LoadStateFromDiskDeployer(OutstandingPersistenceTask outstandingPersistenceTask) {
        this.outstandingPersistenceTask = outstandingPersistenceTask;
        this.oldComputations = new ArrayList<>(outstandingPersistenceTask.getComputationLocations().keySet());
        logger.info("Loaded OutstandingPersistenceTask. Number of computations to schedule: " + oldComputations.size());
    }

    @Override
    public void initialize(Properties streamingProperties) throws CommunicationsException, IOException, MarshallingException, DeploymentException {
        super.initialize(streamingProperties);
        for (ResourceEndpoint endpoint : resourceEndpoints) {
            endpointMap.put(endpoint.getControlEndpoint(), endpoint);
        }
        logger.info("Re-indexed the resource endpoints. Count: " + endpointMap.size());
    }

    @Override
    protected ResourceEndpoint nextResource(Operation op) {
        if (this.completedComputationIndex >= oldComputations.size()) {
            logger.warn("Too many computations to schedule. Index: " + this.completedComputationIndex +
                    ", maximum supported computation count: " + this.oldComputations.size());
            return null;
        }
        String mappedOldComp = oldComputations.get(completedComputationIndex++);
        String oldLocation = outstandingPersistenceTask.getComputationLocations().get(mappedOldComp);
        ResourceEndpoint oldLocEndpoint = endpointMap.get(oldLocation);
        if (oldLocEndpoint == null) {
            logger.error("Old endpoint is not available now. Aborting the deployment. Endpoint: " + oldLocation);
            return null;
        } else {
            // TODO: set the file path to the serialized state in the computation
            return oldLocEndpoint;
        }
    }
}
