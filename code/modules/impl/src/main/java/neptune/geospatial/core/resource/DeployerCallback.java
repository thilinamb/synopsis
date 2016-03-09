package neptune.geospatial.core.resource;

import ds.funnel.topic.TopicDataEvent;
import ds.granules.communication.direct.ChannelReaderCallback;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class DeployerCallback implements ChannelReaderCallback {

    private final Logger logger = Logger.getLogger(DeployerCallback.class);
    private final ManagedResource managedResource;

    public DeployerCallback(ManagedResource managedResource) {
        this.managedResource = managedResource;
    }

    @Override
    public void onEvent(TopicDataEvent data) {
        if(logger.isDebugEnabled()){
            logger.debug("Received deployment callback.");
        }
        managedResource.ackDeployment("");
    }
}
