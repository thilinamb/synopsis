package neptune.geospatial.ft.protocol;

import ds.granules.communication.direct.control.ControlMessage;
import neptune.geospatial.core.protocol.ProtocolTypes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Notify a processor about its new state replication destination
 *
 * @author Thilina Buddhika
 */
public class StateReplicationLevelIncreaseMsg extends ControlMessage {

    private String targetComputation;
    private String previousTopic;
    private String newTopic;

    public StateReplicationLevelIncreaseMsg(String targetComputation, String previousTopic, String newTopic) {
        super(ProtocolTypes.STATE_REPL_LEVEL_INCREASE);
        this.targetComputation = targetComputation;
        this.previousTopic = previousTopic;
        this.newTopic = newTopic;
    }

    public StateReplicationLevelIncreaseMsg() {
        super(ProtocolTypes.STATE_REPL_LEVEL_INCREASE);
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.targetComputation = dataInputStream.readUTF();
        this.previousTopic = dataInputStream.readUTF();
        this.newTopic = dataInputStream.readUTF();
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.targetComputation);
        dataOutputStream.writeUTF(this.previousTopic);
        dataOutputStream.writeUTF(this.newTopic);
    }

    public String getTargetComputation() {
        return targetComputation;
    }

    public String getPreviousTopic() {
        return previousTopic;
    }

    public String getNewTopic() {
        return newTopic;
    }
}
