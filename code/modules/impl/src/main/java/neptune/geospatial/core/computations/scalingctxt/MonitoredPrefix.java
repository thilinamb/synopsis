package neptune.geospatial.core.computations.scalingctxt;

import neptune.geospatial.core.protocol.msg.scalein.ScaleInActivateReq;

/**
 * Represents a monitored prefix.
 * Used to keep track of the message rates for each prefix under the
 * purview of the current computation.
 *
 * @author Thilina Buddhika
 */
public class MonitoredPrefix implements Comparable<MonitoredPrefix> {
    private String prefix;
    private String streamType;
    private long messageCount;
    private double messageRate;
    private boolean isPassThroughTraffic = false;
    private String outGoingStream;
    private String destComputationId;
    private String destResourceCtrlEndpoint;
    private long lastMessageSent = 0;
    private String lastGeoHashSent;
    private long terminationPoint = -1;
    private ScaleInActivateReq activateReq;

    public MonitoredPrefix(String prefix, String streamType) {
        this.prefix = prefix;
        this.streamType = streamType;
    }

    @Override
    public int compareTo(MonitoredPrefix o) {
        // ascending sort based on input rates
        if (this.messageRate == o.messageRate) {
            return this.prefix.compareTo(o.prefix);
        } else {
            return (int) (this.messageRate - o.messageRate);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MonitoredPrefix that = (MonitoredPrefix) o;
        return prefix.equals(that.prefix) && streamType.equals(that.streamType);
    }

    @Override
    public int hashCode() {
        int result = prefix.hashCode();
        result = 31 * result + streamType.hashCode();
        return result;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getStreamType() {
        return streamType;
    }

    public void incrementMessageCount() {
        this.messageCount++;
    }

    public void updateMessageRate(double timeElapsed) {
        this.messageRate = this.messageCount * 1000.0 / timeElapsed;
        this.messageCount = 0;
    }

    public boolean getIsPassThroughTraffic() {
        return isPassThroughTraffic;
    }

    public double getMessageRate() {
        return messageRate;
    }

    public void setIsPassThroughTraffic(boolean isPassThroughTraffic) {
        this.isPassThroughTraffic = isPassThroughTraffic;
    }

    public String getOutGoingStream() {
        return outGoingStream;
    }

    public void setOutGoingStream(String outGoingStream) {
        this.outGoingStream = outGoingStream;
    }

    public String getDestComputationId() {
        return destComputationId;
    }

    public void setDestComputationId(String destComputationId) {
        this.destComputationId = destComputationId;
    }

    public String getDestResourceCtrlEndpoint() {
        return destResourceCtrlEndpoint;
    }

    public void setDestResourceCtrlEndpoint(String destResourceCtrlEndpoint) {
        this.destResourceCtrlEndpoint = destResourceCtrlEndpoint;
    }

    public long getLastMessageSent() {
        return lastMessageSent;
    }

    public void setLastMessageSent(long lastMessageSent) {
        this.lastMessageSent = lastMessageSent;
    }

    public String getLastGeoHashSent() {
        return lastGeoHashSent;
    }

    public void setLastGeoHashSent(String lastGeoHashSent) {
        this.lastGeoHashSent = lastGeoHashSent;
    }

    public long getTerminationPoint() {
        return terminationPoint;
    }

    public void setTerminationPoint(long terminationPoint) {
        this.terminationPoint = terminationPoint;
    }

    public ScaleInActivateReq getActivateReq() {
        return activateReq;
    }

    public void setActivateReq(ScaleInActivateReq activateReq) {
        this.activateReq = activateReq;
    }
}

