package neptune.geospatial.core.query;

import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.protocol.msg.client.TargetQueryResponse;
import neptune.geospatial.core.protocol.msg.client.TargetedQueryRequest;
import neptune.geospatial.graph.operators.SketchProcessor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

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
        byte[] response;
        if (queryReq.getQueryId() >= 0) {
            response = streamProcessor.query(queryReq.getQuery());
        } else {
            String prefix = new String(queryReq.getQuery());
            ByteBuffer byteBuff;
            double memConsumption = ((SketchProcessor) streamProcessor).getMemoryConsumptionForPrefixSlow(prefix);
            byteBuff = ByteBuffer.allocate(Double.BYTES);
            byteBuff.putDouble(memConsumption);
            byteBuff.flip();
            response = byteBuff.array();
        }
        long endTime = System.nanoTime();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Query evaluated. Query id: %d, Eval. time (ns): %d",
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
