package neptune.geospatial.core.protocol.processors.scalout;

import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.streaming.core.exception.StreamingDatasetException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleOutRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutLockRequest;
import neptune.geospatial.core.protocol.msg.scaleout.ScaleOutResponse;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.core.resource.ManagedResource;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Handles {@code ScaleOutResponse} from the deployer
 *
 * @author Thilina Buddhika
 */
public class ScaleOutResponseProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleOutResponseProcessor.class);

    @Override
    public void process(ControlMessage ctrlMessage, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleOutResponse scaleOutResp = (ScaleOutResponse) ctrlMessage;
        if (!scaleOutResp.isSuccess()) {
            logger.warn("Failed scale out request to deployer. Exiting.");
            streamProcessor.releaseMutex();
            return;
        }
        if (scaleOutResp.getPrefixOnlyScaleOutOpId() > 0) {
            streamProcessor.ackPrefixOnlyScaleOut(scaleOutResp.getPrefixOnlyScaleOutOpId());
            PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(scaleOutResp.getInResponseTo());
            for (String prefix : pendingReq.getPrefixes()) {
                MonitoredPrefix monitoredPrefix;
                synchronized (scalingContext) {
                    monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
                    monitoredPrefix.setIsPassThroughTraffic(true);
                    monitoredPrefix.setDestComputationId(scaleOutResp.getNewComputationId());
                    monitoredPrefix.setDestResourceCtrlEndpoint(scaleOutResp.getNewLocationURL());
                    monitoredPrefix.setOutGoingStream(pendingReq.getStreamId());
                }
            }
            streamProcessor.onSuccessfulScaleOut(pendingReq.getPrefixes());
            scalingContext.completeScalingOut(scaleOutResp.getInResponseTo());
            return;
        }
        PendingScaleOutRequest pendingReq = scalingContext.getPendingScaleOutRequest(scaleOutResp.getInResponseTo());
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        if (pendingReq != null) {
            logger.info(String.format("[%s] New scale out location: %s, New comp: %s", instanceIdentifier,
                    scaleOutResp.getNewLocationURL(), scaleOutResp.getNewComputationId()));
            try {
                // send a dummy message, just to ensure the new computation is activated.
                MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(pendingReq.getPrefixes().get(0));
                GeoHashIndexedRecord record = new GeoHashIndexedRecord(monitoredPrefix.getLastGeoHashSent(),
                        monitoredPrefix.getPrefix().length() + 1, -1, System.currentTimeMillis(), new byte[0]);
                streamProcessor.emit(pendingReq.getStreamId(), record);
            } catch (StreamingDatasetException e) {
                logger.error("Error sending a message", e);
                streamProcessor.releaseMutex();
                return;
            }
            ScaleOutLockRequest lockRequest = new ScaleOutLockRequest(scaleOutResp.getInResponseTo(),
                    instanceIdentifier, scaleOutResp.getNewComputationId());
            try {
                SendUtility.sendControlMessage(scaleOutResp.getNewLocationURL(), lockRequest);
            } catch (CommunicationsException | IOException e) {
                logger.error("Error when sending out the lock request.", e);
                streamProcessor.releaseMutex();
                try {
                    ManagedResource.getInstance().scalingOperationComplete(instanceIdentifier);
                } catch (NIException ignore) {

                }
            }

        } else {
            logger.warn("Invalid trigger scaleOutResp for the prefix. Request Id: " + scaleOutResp.getInResponseTo());
        }
    }
}
