package synopsis.client.persistence;

import ds.funnel.data.format.FormatReader;
import ds.funnel.data.format.FormatWriter;
import ds.granules.neptune.interfere.core.NIException;
import neptune.geospatial.benchmarks.sketch.ExtendedSketchProcessorWithLogging;
import neptune.geospatial.core.resource.ManagedResource;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class LoadStateFromDiskOperator extends ExtendedSketchProcessorWithLogging {

    private String serializedStateLocation;
    private Logger logger = Logger.getLogger(LoadStateFromDiskOperator.class);

    public LoadStateFromDiskOperator() {
        super();
    }

    public void setSerializedStateLocation(String serializedStateLocation) {
        this.serializedStateLocation = serializedStateLocation;
    }

    @Override
    protected void deserializeMemberVariables(FormatReader formatReader) {
        super.deserializeMemberVariables(formatReader);
        this.serializedStateLocation = formatReader.readString();
        try {
            ManagedResource.getInstance().registerStreamProcessor(this);
            logger.info("Serialized location: " + this.serializedStateLocation);
        } catch (NIException e) {
            logger.error("Error registering at Managed Resource.", e);
        }
    }

    @Override
    protected void serializedMemberVariables(FormatWriter formatWriter) {
        super.serializedMemberVariables(formatWriter);
        formatWriter.writeString(this.serializedStateLocation);
    }
}
