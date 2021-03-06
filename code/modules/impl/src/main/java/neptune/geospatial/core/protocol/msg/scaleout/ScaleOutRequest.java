package neptune.geospatial.core.protocol.msg.scaleout;


import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Triggers scaling up/down at the controller.
 * ScalableStreamSource -> Deployer
 *
 * @author Thilina Buddhika
 */
public class ScaleOutRequest extends ControlMessage {

    private String currentComputation;
    private String streamId;
    private String topic;
    private String streamType;
    private double requiredMemory = 0;
    private int prefixOnlyScaleOutOperationId = -1;
    private String[] prefixes;

    public ScaleOutRequest() {
        super(ProtocolTypes.SCALE_OUT_REQ);
    }

    public ScaleOutRequest(String currentComputation, String streamId, String topic, String messageType,
                           String[] prefixes) {
        super(ProtocolTypes.SCALE_OUT_REQ);
        this.currentComputation = currentComputation;
        this.streamId = streamId;
        this.topic = topic;
        this.streamType = messageType;
        this.prefixes = prefixes;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.currentComputation = dataInputStream.readUTF();
        this.streamId = dataInputStream.readUTF();
        this.topic = dataInputStream.readUTF();
        this.streamType = dataInputStream.readUTF();
        this.requiredMemory = dataInputStream.readDouble();
        this.prefixOnlyScaleOutOperationId = dataInputStream.readInt();
        int prefixCount = dataInputStream.readInt();
        this.prefixes = new String[prefixCount];
        for(int i = 0; i < prefixCount; i++){
            this.prefixes[i] = dataInputStream.readUTF();
        }
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.currentComputation);
        dataOutputStream.writeUTF(this.streamId);
        dataOutputStream.writeUTF(this.topic);
        dataOutputStream.writeUTF(this.streamType);
        dataOutputStream.writeDouble(this.requiredMemory);
        dataOutputStream.writeInt(this.prefixOnlyScaleOutOperationId);
        dataOutputStream.writeInt(this.prefixes.length);
        for (int i = 0; i < this.prefixes.length; i++){
            dataOutputStream.writeUTF(this.prefixes[i]);
        }
    }

    public String getCurrentComputation() {
        return currentComputation;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getTopic() {
        return topic;
    }

    public String getStreamType() {
        return streamType;
    }

    public double getRequiredMemory() {
        return requiredMemory;
    }

    public void setRequiredMemory(double requiredMemory) {
        this.requiredMemory = requiredMemory;
    }

    public int getPrefixOnlyScaleOutOperationId() {
        return prefixOnlyScaleOutOperationId;
    }

    public void setPrefixOnlyScaleOutOperationId(int prefixOnlyScaleOutOperationId) {
        this.prefixOnlyScaleOutOperationId = prefixOnlyScaleOutOperationId;
    }

    public String[] getPrefixes() {
        return prefixes;
    }
}
