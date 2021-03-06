package neptune.geospatial.graph;

import ds.granules.Granules;
import ds.granules.streaming.core.Job;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ParamsReader;
import neptune.geospatial.core.deployer.GeoSpatialDeployer;
import neptune.geospatial.graph.operators.RecordCounter;
import neptune.geospatial.graph.operators.StreamIngester;
import neptune.geospatial.partitioner.GeoHashPartitioner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Properties;

/**
 * Defines the stream processing graph by connecting various operators
 * through streams.
 *
 * @author Thilina Buddhika
 */
public class Graph {

    private static Logger logger = Logger.getLogger(Graph.class);

    public static void main(String[] args) {
        ParamsReader paramsReader = Granules.getParamsReader();
        String configLocation = "conf/ResourceConfig.txt";

        if (args.length != 0) {
            configLocation = args[0];
        }

        try {
            Properties resourceProps = new Properties();

            /* Read properties from config file, if it exists. */
            File configFile = new File(configLocation);
            if (configFile.exists()) {
                resourceProps = paramsReader.getProperties(configLocation);
            }

            NeptuneRuntime.initialize(resourceProps);
            Job job = new Job("GeoSpatial-Data-Processing-Graph", GeoSpatialDeployer.getDeployer());

            // vertices
            Properties senderProps = new Properties();
            senderProps.put(ds.granules.util.Constants.StreamBaseProperties.BUFFER_SIZE, Integer.toString(1800));
            job.addStreamSource("ingester", StreamIngester.class, 1, senderProps);

            Properties processorProps = new Properties();
            processorProps.put(ds.granules.util.Constants.StreamBaseProperties.BUFFER_SIZE, Integer.toString(0));
            job.addStreamProcessor("record-counter", RecordCounter.class, 1, processorProps);

            // edges
            job.addLink("ingester", "record-counter", Constants.Streams.GEO_HASH_INDEXED_RECORDS,
                    GeoHashPartitioner.class.getName());
            job.deploy();

        } catch (Exception e) {
            logger.error("Error deploying the graph.", e);
        }
    }
}
