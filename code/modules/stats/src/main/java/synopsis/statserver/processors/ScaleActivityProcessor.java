package synopsis.statserver.processors;

import neptune.geospatial.stat.StatConstants;
import synopsis.statserver.ScalingActivity;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author Thilina Buddhika
 */
public class ScaleActivityProcessor {

    public String getOutputFileName() {
        return "scale-activity";
    }

    public void process(List<ScalingActivity> completedActivities, BufferedWriter bufferedWriter) throws IOException {
        // sort based on start time of the activity
        Collections.sort(completedActivities);
        while (completedActivities.size() > 0) {
            ScalingActivity activity = completedActivities.remove(0);
            bufferedWriter.write(activity.getInstanceId() + "," + activity.getHostname() + "," +
                    activity.getStartTime() + "," + activity.getEndTime() + "," +
                    (activity.isScaleActivityType() == StatConstants.ScaleActivityType.SCALE_OUT ? "scale-out" : "scale-in") + "\n");
        }
    }
}
