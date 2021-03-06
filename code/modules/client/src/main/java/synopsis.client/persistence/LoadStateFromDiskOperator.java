package synopsis.client.persistence;

import ds.funnel.data.format.FormatReader;
import ds.funnel.data.format.FormatWriter;
import ds.granules.neptune.interfere.core.NIException;
import neptune.geospatial.benchmarks.sketch.ExtendedSketchProcessorWithLogging;
import neptune.geospatial.core.resource.ManagedResource;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * @author Thilina Buddhika
 */
public class LoadStateFromDiskOperator extends ExtendedSketchProcessorWithLogging {

    private String serializedStateLocation;
    private Logger logger = Logger.getLogger(LoadStateFromDiskOperator.class);

    public LoadStateFromDiskOperator() {
        super();
    }

    void setSerializedStateLocation(String serializedStateLocation) {
        this.serializedStateLocation = serializedStateLocation;
    }

    @Override
    protected void deserializeMemberVariables(FormatReader formatReader) {
        super.deserializeMemberVariables(formatReader);
        this.serializedStateLocation = formatReader.readString();
        // register with resource to receive control messages
        try {
            ManagedResource.getInstance().registerStreamProcessor(this);
        } catch (NIException e) {
            logger.error("Error registering at Managed Resource.", e);
            return;
        }
        // deserialize state
        logger.info("Serialized location: " + this.serializedStateLocation);
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(this.serializedStateLocation);
            dis = new DataInputStream(fis);
            long t1 = System.currentTimeMillis();
            deserialize(dis);
            long t2 = System.currentTimeMillis();
            File f = new File(this.serializedStateLocation);
            long fileSize = f.length();
            //logger.info("Successfully deserialized state from disk. Time Elapsed (seconds): " + (t2 - t1) / 1000.0 +
            //        ", file Size: " + fileSize);
            System.out.println((t2 - t1) + "," + fileSize + "," + getMemoryConsumptionForAllPrefixes() + "," + getLeafCount());
        } catch (FileNotFoundException e) {
            logger.error("File not found. File: " + this.serializedStateLocation, e);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
                if (dis != null) {
                    dis.close();
                }
            } catch (IOException e) {
                logger.error("Error closing input streams.", e);
            }
        }
    }

    @Override
    protected void serializedMemberVariables(FormatWriter formatWriter) {
        super.serializedMemberVariables(formatWriter);
        formatWriter.writeString(this.serializedStateLocation);
    }

    @Override
    protected boolean publishData() {
        return true;
    }
}
