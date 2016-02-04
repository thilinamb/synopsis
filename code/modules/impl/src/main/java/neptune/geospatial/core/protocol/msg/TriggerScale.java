package neptune.geospatial.core.protocol.msg;


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
public class TriggerScale extends ControlMessage {

    private String currentComputation;
    private String streamId;
    private String topic;
    private String streamType;

    public TriggerScale() {
        super(ProtocolTypes.TRIGGER_SCALING);
    }

    public TriggerScale(String currentComputation, String streamId, String topic, String messageType) {
        super(ProtocolTypes.TRIGGER_SCALING);
        this.currentComputation = currentComputation;
        this.streamId = streamId;
        this.topic = topic;
        this.streamType = messageType;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.currentComputation = dataInputStream.readUTF();
        this.streamId = dataInputStream.readUTF();
        this.topic = dataInputStream.readUTF();
        this.streamType = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.currentComputation);
        dataOutputStream.writeUTF(this.streamId);
        dataOutputStream.writeUTF(this.topic);
        dataOutputStream.writeUTF(this.streamType);
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
}
