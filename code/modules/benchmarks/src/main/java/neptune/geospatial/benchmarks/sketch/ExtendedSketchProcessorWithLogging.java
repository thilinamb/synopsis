package neptune.geospatial.benchmarks.sketch;

import neptune.geospatial.core.computations.SketchProcessor;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class ExtendedSketchProcessorWithLogging extends SketchProcessor {

    private Logger logger = Logger.getLogger(ExtendedSketchProcessorWithLogging.class);

    @Override
    public void onStartOfScaleOut() {
        logger.info(String.format("[%s] Starting to scale out.", getInstanceIdentifier()));
    }

    @Override
    public void onStartOfScaleIn() {
        logger.info(String.format("[%s] Starting to scale in.", getInstanceIdentifier()));
    }

    @Override
    public void onSuccessfulScaleOut(List<String> prefixes) {
        logger.info(String.format("[%s] Completed scaling out. Prefix count: %d", getInstanceIdentifier(), prefixes.size()));
    }

    @Override
    public void onSuccessfulScaleIn(List<String> prefixes) {
        logger.info(String.format("[%s] Completed scaling in. Prefix count: %d", getInstanceIdentifier(), prefixes.size()));
    }
}
