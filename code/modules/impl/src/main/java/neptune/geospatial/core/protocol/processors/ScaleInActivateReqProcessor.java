package neptune.geospatial.core.protocol.processors;

import com.hazelcast.core.IMap;
import ds.granules.communication.direct.control.ControlMessage;
import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.FullQualifiedComputationAddr;
import neptune.geospatial.core.computations.scalingctxt.MonitoredPrefix;
import neptune.geospatial.core.computations.scalingctxt.PendingScaleInRequest;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.ScaleInActivateReq;
import neptune.geospatial.core.protocol.msg.StateTransferMsg;
import neptune.geospatial.hazelcast.type.SketchLocation;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleInActivateReqProcessor implements ProtocolProcessor {

    private Logger logger = Logger.getLogger(ScaleInActivateReqProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {

        ScaleInActivateReq activationReq = (ScaleInActivateReq) ctrlMsg;
        String prefix = activationReq.getPrefix();
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();

        if (scalingContext.getPendingScalingInRequest(prefix) == null) {
            logger.warn("Invalid ScaleInActivateReq for prefix: " + prefix);
        } else {
            String lastMessagePrefix = streamProcessor.getPrefix(activationReq.getLastGeoHashSent(),
                    activationReq.getCurrentPrefixLength());
            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(lastMessagePrefix);
            if (monitoredPrefix != null) {
                if (monitoredPrefix.getLastMessageSent() == activationReq.getLastMessageSent()) {
                    // we have already seen this message.
                    propagateScaleInActivationRequests(activationReq, streamProcessor, scalingContext);
                } else {
                    monitoredPrefix.setTerminationPoint(activationReq.getLastMessageSent());
                    monitoredPrefix.setActivateReq(activationReq);
                }
            } else { // it is possible that the message has delivered yet, especially if there is a backlog
                monitoredPrefix = new MonitoredPrefix(lastMessagePrefix, null);
                monitoredPrefix.setTerminationPoint(activationReq.getLastMessageSent());
                monitoredPrefix.setActivateReq(activationReq);
                scalingContext.addMonitoredPrefix(lastMessagePrefix, monitoredPrefix);
                try {
                    IMap<String, SketchLocation> prefMap = streamProcessor.getHzInstance().getMap(GeoHashPrefixTree.PREFIX_MAP);
                    prefMap.put(lastMessagePrefix, new SketchLocation(instanceIdentifier,
                            RivuletUtil.getCtrlEndpoint(), SketchLocation.MODE_REGISTER_NEW_PREFIX));
                } catch (GranulesConfigurationException e) {
                    logger.error("Error publishing to Hazelcast.", e);
                }
            }
        }
    }

    private void propagateScaleInActivationRequests(ScaleInActivateReq activationReq,
                                                    AbstractGeoSpatialStreamProcessor streamProcessor,
                                                    ScalingContext scalingContext) {
        String prefix = activationReq.getPrefix();
        PendingScaleInRequest pendingReq = scalingContext.getPendingScalingInRequest(prefix);
        String instanceIdentifier = streamProcessor.getInstanceIdentifier();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("[%s] Received ScaleInActivateReq for prefix: %s", instanceIdentifier,
                    prefix));
        }
        for (String lockedPrefix : pendingReq.getSentOutRequests().keySet()) {
            // disable pass-through
            MonitoredPrefix monitoredPrefix = scalingContext.getMonitoredPrefix(prefix);
            monitoredPrefix.setIsPassThroughTraffic(false);
            FullQualifiedComputationAddr reqInfo = pendingReq.getSentOutRequests().get(lockedPrefix);

            try {
                ScaleInActivateReq scaleInActivateReq = new ScaleInActivateReq(prefix, reqInfo.getComputationId(),
                        monitoredPrefix.getLastMessageSent(), monitoredPrefix.getLastGeoHashSent(), lockedPrefix.length(),
                        activationReq.getOriginNodeOfScalingOperation(),
                        activationReq.getOriginComputationOfScalingOperation());
                SendUtility.sendControlMessage(reqInfo.getCtrlEndpointAddr(), scaleInActivateReq);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] Propagating ScaleInActivateReq to children. " +
                                    "Parent prefix: %s, Child prefix: %s, Last message processed: %d",
                            instanceIdentifier, prefix, lockedPrefix, monitoredPrefix.getLastMessageSent()));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending ScaleInActivationRequest to " + reqInfo.getCtrlEndpointAddr(), e);
            }
        }
        for (String localPrefix : pendingReq.getLocallyProcessedPrefixes()) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("[%s] ScaleInActivationReq for locally processed prefix. " +
                                "Parent Prefix: %s, Child Prefix: %s, Last Processed Sent: %d",
                        instanceIdentifier, prefix, localPrefix, activationReq.getLastMessageSent()));
            }
            // get the state for the prefix in the serialized form
            byte[] state = streamProcessor.split(localPrefix);
            StateTransferMsg stateTransMsg = new StateTransferMsg(localPrefix, prefix, state,
                    activationReq.getOriginComputationOfScalingOperation(), instanceIdentifier,
                    StateTransferMsg.SCALE_IN);
            try {
                SendUtility.sendControlMessage(activationReq.getOriginNodeOfScalingOperation(), stateTransMsg);
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[%s] StateTransfer for local prefix: %s. Key prefix: %s.",
                            instanceIdentifier, localPrefix, prefix));
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error when sending out the StateTransfer message to " +
                        activationReq.getOriginNodeOfScalingOperation());
            }
        }
    }
}
