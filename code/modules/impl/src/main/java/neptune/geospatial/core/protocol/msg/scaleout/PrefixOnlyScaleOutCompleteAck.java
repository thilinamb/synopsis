package neptune.geospatial.core.protocol.msg.scaleout;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class PrefixOnlyScaleOutCompleteAck extends ControlMessage {

    private String ingesterId;

    public PrefixOnlyScaleOutCompleteAck() {
        super(ProtocolTypes.PREFIX_ONLY_SCALE_OUT_COMPLETE);
    }

    public PrefixOnlyScaleOutCompleteAck(String ingesterId) {
        super(ProtocolTypes.PREFIX_ONLY_SCALE_OUT_COMPLETE);
        this.ingesterId = ingesterId;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.ingesterId = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.ingesterId);
    }

    public String getIngesterId() {
        return ingesterId;
    }
}
