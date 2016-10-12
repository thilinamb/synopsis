package neptune.geospatial.core.protocol.processors.client;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetedQueryRequest;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class TargetedQueryProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(TargetedQueryProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        TargetedQueryRequest queryReq = (TargetedQueryRequest) ctrlMsg;
        long startTime = System.nanoTime();
        byte[] response = streamProcessor.query(queryReq.getQuery());
        long endTime = System.nanoTime();
        logger.info(String.format("[%s] Query evaluated. Query id: %d, Eval. time: %d",
                streamProcessor.getInstanceIdentifier(), queryReq.getQueryId(), (endTime - startTime)));
        TargetQueryResponse queryResponse = new TargetQueryResponse(queryReq.getQueryId(),
                streamProcessor.getInstanceIdentifier(), response, (endTime - startTime));
        try {
            SendUtility.sendControlMessage(queryReq.getClientAddr(), queryResponse);
        } catch (CommunicationsException | IOException e) {
            logger.error("Error sending back the query response to the client. Query id: " + queryReq.getQueryId(), e);
        }
    }
}
