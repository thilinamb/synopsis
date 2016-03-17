package neptune.geospatial.core.protocol.processors;

import com.hazelcast.core.IQueue;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.neptune.interfere.core.NIException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleOutRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleOutCompleteAck;
import neptune.geospatial.core.resource.ManagedResource;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class ScaleOutCompleteProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleOutCompleteProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleOutCompleteAck ack = (ScaleOutCompleteAck) ctrlMsg;
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(ack.getKey());
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();

        if (pendingReq != null) {
            int ackCount = pendingReq.incrementAndGetAckCount();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] Received a ScaleOutCompleteAck. Sent Count: %d, Received Count: %d",
                        instanceIdentifier, pendingReq.getPrefixes().size(), ackCount));
            }
            // update prefix tree
            /*IMap<String, SketchLocation> prefMap = hzInstance.getMap(GeoHashPrefixTree.PREFIX_MAP);
            MonitoredPrefix monitoredPrefix = monitoredPrefixMap.get(ack.getPrefix());
            prefMap.put(ack.getPrefix(), new SketchLocation(monitoredPrefix.destComputationId,
                    monitoredPrefix.destResourceCtrlEndpoint,
                    SketchLocation.MODE_SCALE_OUT));
            */
            if (ackCount == pendingReq.getPrefixes().size()) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("[%s] Scaling out complete for now.", instanceIdentifier));
                    }
                    // TODO: temporary fix to track dynamic scaling. Remove this and uncomment above after the micro-benchmark
                    IQueue<Integer> scalingMonitoringQueue = streamProcessor.getHzInstance().getQueue("scaling-monitor");
                    scalingMonitoringQueue.add(ackCount);
                    scalingContext.completeScalingOut(ack.getKey());
                    streamProcessor.releaseMutex();
                    ManagedResource.getInstance().scalingOperationComplete(instanceIdentifier);
                } catch (NIException ignore) {

                }
            }
        } else {
            logger.warn("Invalid ScaleOutCompleteAck for " + ack.getKey());
        }
    }
}
