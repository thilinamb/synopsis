package synopsis.client.persistence;

import com.google.gson.Gson;
import ds.granules.Granules;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.DeploymentException;
import ds.granules.exception.MarshallingException;
import ds.granules.operation.Operation;
import ds.granules.streaming.core.Job;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ParamsReader;
import neptune.geospatial.core.deployer.GeoSpatialDeployer;
import neptune.geospatial.core.protocol.msg.client.UpdatePrefixTreeReq;
import neptune.geospatial.graph.Constants;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import neptune.geospatial.util.trie.Node;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * @author Thilina Buddhika
 */
public class LoadStateFromDiskDeployer extends GeoSpatialDeployer {

    private final static Logger logger = Logger.getLogger(LoadStateFromDiskDeployer.class);

    private final OutstandingPersistenceTask outstandingPersistenceTask;
    private List<String> oldComputations = new ArrayList<>();
    private int completedComputationIndex = 0;
    private Map<String, ResourceEndpoint> endpointMap = new HashMap<>();
    private Map<String, String> oldCompToNewMap = new HashMap<>();
    private List<ResourceEndpoint> currentDeploymentNodes = new ArrayList<>();

    private LoadStateFromDiskDeployer(OutstandingPersistenceTask outstandingPersistenceTask) {
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
        if (!(op instanceof LoadStateFromDiskOperator)) {
            return super.nextResource(op);
        }

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
            String serializedStateLocation = outstandingPersistenceTask.getStorageLocations().get(mappedOldComp);
            ((LoadStateFromDiskOperator) op).setSerializedStateLocation(
                    serializedStateLocation);
            oldCompToNewMap.put(mappedOldComp, op.getInstanceIdentifier());
            currentDeploymentNodes.add(oldLocEndpoint);
            logger.info(String.format("Mapped a new comp. New comp. id: %s, Old comp. id: %s, " +
                            "Location: %s, Serialized State Loc.: %s", mappedOldComp,
                    op.getInstanceIdentifier(), oldLocation, serializedStateLocation));
            return oldLocEndpoint;
        }
    }

    private void updatePrefixTree() throws DeploymentException {
        GeoHashPrefixTree prefixTree = GeoHashPrefixTree.getInstance();
        try {
            prefixTree.deserialize(outstandingPersistenceTask.getSerializedPrefixTree());
            updateNode(prefixTree.getRoot());
            outstandingPersistenceTask.setSerializedPrefixTree(prefixTree.serialize());
        } catch (IOException e) {
            throw new DeploymentException("Error deserializing prefix tree.", e);
        }
    }

    private void updateNode(Node node) throws DeploymentException {
        if (!node.isRoot()) {
            String oldComp = node.getComputationId();
            if (oldCompToNewMap.containsKey(oldComp)) {
                node.setComputationId(oldCompToNewMap.get(oldComp));
                logger.info("Replaced " + oldComp + " with " + oldCompToNewMap.get(oldComp));
            } else {
                throw new DeploymentException("Invalid computation in prefix tree: " + oldComp);
            }
        }
        for (Node child : node.getChildNodes().values()) {
            updateNode(child);
        }
    }

    private byte[] getUpdatedPrefixTree() {
        return this.outstandingPersistenceTask.getSerializedPrefixTree();
    }

    private List<ResourceEndpoint> getDeploymentLocations() {
        return this.currentDeploymentNodes;
    }

    public static void main(String[] args) {
        ParamsReader paramsReader = Granules.getParamsReader();
        String configLocation = "conf/ResourceConfig.txt";
        String serializedDeploymentPlanLoc = null;

        if (args.length >= 5) {
            configLocation = args[0];
            serializedDeploymentPlanLoc = args[4];
        }

        logger.info("Using deployment path plan: " + serializedDeploymentPlanLoc);
        OutstandingPersistenceTask outstandingPersistenceTask = null;
        if (serializedDeploymentPlanLoc != null) {
            try {
                FileInputStream fis = new FileInputStream(serializedDeploymentPlanLoc);
                DataInputStream dis = new DataInputStream(fis);
                int length = dis.readInt();
                logger.info("Serialized deployment plan size: " + length);
                byte[] serializedBytes = new byte[length];
                dis.readFully(serializedBytes);
                String serializedString = new String(serializedBytes);
                Gson gson = new Gson();
                outstandingPersistenceTask = gson.fromJson(serializedString, OutstandingPersistenceTask.class);
            } catch (IOException e) {
                logger.error("Error reading the deployment plan.", e);
                System.exit(-1);
            }
        } else {
            System.err.println("Location of the serialized deployment plan is not provided. Exiting.");
            System.exit(-1);
        }

        LoadStateFromDiskDeployer deployer = new LoadStateFromDiskDeployer(outstandingPersistenceTask);
        int compCount = outstandingPersistenceTask.getTotalComputationCount();
        logger.info("Deploying " + compCount + " processors.");
        try {
            Properties resourceProps = new Properties();

            /* Read properties from config file, if it exists. */
            File configFile = new File(configLocation);
            if (configFile.exists()) {
                resourceProps = paramsReader.getProperties(configLocation);
            }

            NeptuneRuntime.initialize(resourceProps);

            deployer.initialize(NeptuneRuntime.getInstance().getProperties());
            Job job = new Job("NOAA-Data-Processing-Graph", deployer);

            // vertices
            Properties senderProps = new Properties();
            senderProps.put(ds.granules.util.Constants.StreamBaseProperties.BUFFER_SIZE, Integer.toString(1024 * 1024));
            job.addStreamSource("ingester", PartialDatasetIngester.class, 12, senderProps);

            Properties processorProps = new Properties();
            processorProps.put(ds.granules.util.Constants.StreamBaseProperties.BUFFER_SIZE, Integer.toString(0));
            //job.addStreamProcessor("stream-processor", SketchProcessor.class, INITIAL_PROCESSOR_COUNT, processorProps);
            job.addStreamProcessor("stream-processor", LoadStateFromDiskOperator.class, compCount,
                    processorProps);
            // edges
            job.addLink("ingester", "stream-processor", Constants.Streams.NOAA_DATA_STREAM,
                    GeoHashPartitioner.class.getName());

            job.deploy();

            // update the prefix tree and propagate it
            deployer.updatePrefixTree();
            byte[] updatedPrefixTree = deployer.getUpdatedPrefixTree();
            UpdatePrefixTreeReq updatePrefixTreeReq = new UpdatePrefixTreeReq(updatedPrefixTree);

            for(ResourceEndpoint endpoint: deployer.getDeploymentLocations()){
                SendUtility.sendControlMessage(endpoint.getControlEndpoint(), updatePrefixTreeReq);
            }

        } catch (Exception e) {
            logger.error("Error deploying the graph.", e);
        }
    }

}
