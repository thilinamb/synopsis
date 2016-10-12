package neptune.geospatial.core.protocol.processors.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.client.TargetedQueryRequest;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class TargetedQueryProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(TargetedQueryProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        TargetedQueryRequest queryReq = (TargetedQueryRequest) ctrlMsg;
        byte[] response = streamProcessor.query(queryReq.getQuery());
        // todo: send it back to the client
    }
}
