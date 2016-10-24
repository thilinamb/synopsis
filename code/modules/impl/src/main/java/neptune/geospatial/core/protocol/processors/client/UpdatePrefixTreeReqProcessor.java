package neptune.geospatial.core.protocol.processors.client;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;
import neptune.geospatial.core.protocol.msg.client.UpdatePrefixTreeReq;
import neptune.geospatial.core.protocol.processors.ProtocolProcessor;
import neptune.geospatial.util.trie.GeoHashPrefixTree;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class UpdatePrefixTreeReqProcessor implements ProtocolProcessor {

    private final Logger logger = Logger.getLogger(UpdatePrefixTreeReqProcessor.class);

    @Override
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor) {
        UpdatePrefixTreeReq updatePrefixTreeReq = (UpdatePrefixTreeReq) ctrlMsg;
        GeoHashPrefixTree prefixTree = GeoHashPrefixTree.getInstance();
        try {
            prefixTree.deserialize(updatePrefixTreeReq.getUpdatedPrefixTree());
            logger.info("[" + streamProcessor.getInstanceIdentifier() + "] Successfully deserialized prefix tree.");
        } catch (IOException e) {
            logger.error("Error deserializing prefix tree.", e);
        }
    }
}
