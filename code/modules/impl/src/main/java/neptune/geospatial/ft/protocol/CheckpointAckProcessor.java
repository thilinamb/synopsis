package neptune.geospatial.ft.protocol;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;

/**
 * @author Thilina Buddhika
 */
public class CheckpointAckProcessor implements ProtocolProcessor {
    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        CheckpointAck checkpointAck = (CheckpointAck) ctrlMsg;
        streamProcessor.handleAckStatePersistence(checkpointAck);
    }
}
