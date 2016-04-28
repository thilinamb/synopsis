package synopsis.statserver;

/**
 * @author Thilina Buddhika
 */
public class ScalingActivity implements Comparable<ScalingActivity> {
    private String instanceId;
    private boolean scaleActivityType;
    private long startTime;
    private long endTime;

    public ScalingActivity(String instanceId, boolean scaleActivityType, long startTime) {
        this.instanceId = instanceId;
        this.scaleActivityType = scaleActivityType;
        this.startTime = startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScalingActivity that = (ScalingActivity) o;

        if (scaleActivityType != that.scaleActivityType) return false;
        if (startTime != that.startTime) return false;
        return instanceId.equals(that.instanceId);
    }

    @Override
    public int hashCode() {
        int result = instanceId.hashCode();
        result = 31 * result + (scaleActivityType ? 1 : 0);
        result = 31 * result + (int) (startTime ^ (startTime >>> 32));
        return result;
    }

    @Override
    public int compareTo(ScalingActivity o) {
        if (this.equals(o)) return 0;
        return new Long(this.startTime).compareTo(o.startTime);
    }

    public String getInstanceId() {
        return instanceId;
    }

    public boolean isScaleActivityType() {
        return scaleActivityType;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
}
