package neptune.geospatial.core.protocol.processors;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor;
import neptune.geospatial.core.computations.scalingctxt.ScalingContext;

/**
 * Processes a specific control message in the protocol flow for dynamic scaling
 *
 * @author Thilina Buddhika
 */
public interface ProtocolProcessor {

    /**
     * Processes a control message
     *
     * @param ctrlMsg {@link ds.granules.communication.direct.control.ControlMessage} itself
     * @param scalingContext Instance of the {@link neptune.geospatial.core.computations.scalingctxt.ScalingContext} corresponding to the computation
     * @param streamProcessor {@link neptune.geospatial.core.computations.AbstractGeoSpatialStreamProcessor} object
     */
    public void process(ControlMessage ctrlMsg, ScalingContext scalingContext,
                        AbstractGeoSpatialStreamProcessor streamProcessor);

}
