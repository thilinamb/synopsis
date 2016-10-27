package neptune.geospatial.core.query;

import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetedQueryRequest;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class QueryProcessingTask implements Runnable {

    private TargetedQueryRequest queryReq;
    private AbstractGeoSpatialStreamProcessor streamProcessor;
    private QueryStatReporter queryStatReporter;
    private Logger logger = Logger.getLogger(QueryProcessingTask.class);

    public QueryProcessingTask(TargetedQueryRequest queryRequest, AbstractGeoSpatialStreamProcessor streamProcessor) {
        this.queryReq = queryRequest;
        this.streamProcessor = streamProcessor;
        this.queryStatReporter = QueryStatReporter.getInstance();
    }

    @Override
    public void run() {
        long startTime = System.nanoTime();
        byte[] response = streamProcessor.query(queryReq.getQuery());
        long endTime = System.nanoTime();
        if(logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Query evaluated. Query id: %d, Eval. time: %d",
                    streamProcessor.getInstanceIdentifier(), queryReq.getQueryId(), (endTime - startTime)));
        }
        TargetQueryResponse queryResponse = new TargetQueryResponse(queryReq.getQueryId(),
                streamProcessor.getInstanceIdentifier(), response, (endTime - startTime));
        try {
            SendUtility.sendControlMessage(queryReq.getClientAddr(), queryResponse);
            this.queryStatReporter.record(this.streamProcessor.getInstanceIdentifier(), 1);
        } catch (CommunicationsException | IOException e) {
            logger.error("Error sending back the query response to the client. Query id: " + queryReq.getQueryId(), e);
        }
    }
}
