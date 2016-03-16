package neptune.geospatial.core.computations.scalingctxt;

import java.util.List;

/**
 * Pending Scale Out Requests.
 * A scale out request message is sent to the deployer and we are waiting
 * for a response.
 *
 * @author Thilina Buddhika
 */
public class PendingScaleOutRequest {
    private List<String> prefixes;
    private String streamId;
    private int ackCount;

    public PendingScaleOutRequest(List<String> prefixes, String streamId) {
        this.prefixes = prefixes;
        this.streamId = streamId;
    }

    public List<String> getPrefixes() {
        return prefixes;
    }

    public String getStreamId() {
        return streamId;
    }

    public int getAckCount() {
        return ackCount;
    }
}
