package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response for a scale-in lock request
 *
 * @author Thilina Buddhika
 */
public class ScaleInLockResponse extends ControlMessage {

    private boolean success;
    private String prefix;
    private String computation;
    private List<String> leafPrefixes;

    public ScaleInLockResponse() {
        super(ProtocolTypes.SCALE_IN_LOCK_RESP);
    }

    public ScaleInLockResponse(boolean success, String prefix, String computation, List<String> leafPrefixes) {
        super(ProtocolTypes.SCALE_IN_LOCK_RESP);
        this.success = success;
        this.prefix = prefix;
        this.computation = computation;
        this.leafPrefixes = leafPrefixes;
    }

    @Override
    public void readValues(DataInputStream dis) throws IOException {
        this.success = dis.readBoolean();
        this.prefix = dis.readUTF();
        this.computation = dis.readUTF();
        int leafPrefixCount = dis.readInt();
        if (leafPrefixCount > 0) {
            this.leafPrefixes = new ArrayList<>(leafPrefixCount);
            for (int i = 0; i < leafPrefixCount; i++) {
                this.leafPrefixes.add(dis.readUTF());
            }
        }
    }

    @Override
    public void writeValues(DataOutputStream dos) throws IOException {
        dos.writeBoolean(this.success);
        dos.writeUTF(this.prefix);
        dos.writeUTF(this.computation);
        if (this.leafPrefixes != null) {
            dos.writeInt(this.leafPrefixes.size());
            for (String leafPrefix : leafPrefixes) {
                dos.writeUTF(leafPrefix);
            }
        } else {
            dos.writeInt(0);
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getComputation() {
        return computation;
    }

    public List<String> getLeafPrefixes() {
        return leafPrefixes;
    }
}
