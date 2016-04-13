package neptune.geospatial.ft.protocol;

import ds.funnel.topic.StringTopic;
import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import org.apache.log4j.Logger;

/**
 * Process a {@link StateReplLvlIncreaseMsgProcessor} to update the
 * state replication processors.
 *
 * @author Thilina Buddhika
 */
public class StateReplLvlIncreaseMsgProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(StateReplLvlIncreaseMsgProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        StateReplicationLevelIncreaseMsg replicationLevelIncreaseMsg = (StateReplicationLevelIncreaseMsg) ctrlMsg;
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Processing a replicationLevelIncreaseMsg.", streamProcessor.getInstanceIdentifier()));
        }
        streamProcessor.addNewStateReplicationTopic(new StringTopic(replicationLevelIncreaseMsg.getNewTopic()),
                replicationLevelIncreaseMsg.getNewLocation());
    }
}
