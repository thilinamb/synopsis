package neptune.geospatial.stat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class ScaleActivity extends StatisticsRecord {

    private boolean activityType;
    private boolean eventType;

    public ScaleActivity(String instanceId, boolean activityType, boolean eventType) {
        super(StatConstants.MessageTypes.STAT_ACTIVITY, instanceId);
        this.activityType = activityType;
        this.eventType = eventType;
    }

    public ScaleActivity() {
        super(StatConstants.MessageTypes.STAT_ACTIVITY, null);
    }

    @Override
    public void unmarshall(DataInputStream dataInputStream) throws IOException {
        this.activityType = dataInputStream.readBoolean();
        this.eventType = dataInputStream.readBoolean();
    }

    @Override
    public void marshall(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeBoolean(this.activityType);
        dataOutputStream.writeBoolean(this.eventType);
    }

    public boolean isActivityType() {
        return activityType;
    }

    public boolean isEventType() {
        return eventType;
    }
}
