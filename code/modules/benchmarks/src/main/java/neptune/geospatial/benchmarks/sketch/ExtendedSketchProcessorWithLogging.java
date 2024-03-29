package neptune.geospatial.benchmarks.sketch;

import neptune.geospatial.graph.operators.SketchProcessor;
import neptune.geospatial.stat.ScaleActivity;
import neptune.geospatial.stat.StatClient;
import neptune.geospatial.stat.StatConstants;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class ExtendedSketchProcessorWithLogging extends SketchProcessor {

    private Logger logger = Logger.getLogger(ExtendedSketchProcessorWithLogging.class);
    private StatClient statClient = StatClient.getInstance();
    private BufferedWriter buffW;

    public ExtendedSketchProcessorWithLogging() {
        try {
            buffW = new BufferedWriter(new FileWriter("/tmp/scaling.stat"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStartOfScaleOut() {
        ScaleActivity scaleActivity = new ScaleActivity(getInstanceIdentifier(),
                StatConstants.ScaleActivityType.SCALE_OUT, StatConstants.ScaleActivityEvent.START);
        try {
            buffW.write(System.currentTimeMillis() + "," + "start" + "\n");
            buffW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        statClient.publish(scaleActivity);
        logger.info(String.format("[%s] Starting to scale out.", getInstanceIdentifier()));
    }

    @Override
    public void onStartOfScaleIn() {
        ScaleActivity scaleActivity = new ScaleActivity(getInstanceIdentifier(),
                StatConstants.ScaleActivityType.SCALE_IN, StatConstants.ScaleActivityEvent.START);
        statClient.publish(scaleActivity);
        logger.info(String.format("[%s] Starting to scale in.", getInstanceIdentifier()));
    }

    @Override
    public void onSuccessfulScaleOut(List<String> prefixes) {
        ScaleActivity scaleActivity = new ScaleActivity(getInstanceIdentifier(),
                StatConstants.ScaleActivityType.SCALE_OUT, StatConstants.ScaleActivityEvent.END);
        try {
            buffW.write(System.currentTimeMillis() + "," + "end" + "\n");
            buffW.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        statClient.publish(scaleActivity);
        logger.info(String.format("[%s] Completed scaling out. Prefix count: %d", getInstanceIdentifier(), prefixes.size()));
    }

    @Override
    public void onSuccessfulScaleIn(List<String> prefixes) {
        ScaleActivity scaleActivity = new ScaleActivity(getInstanceIdentifier(),
                StatConstants.ScaleActivityType.SCALE_IN, StatConstants.ScaleActivityEvent.END);
        statClient.publish(scaleActivity);
        logger.info(String.format("[%s] Completed scaling in. Prefix count: %d", getInstanceIdentifier(), prefixes.size()));
    }
}
