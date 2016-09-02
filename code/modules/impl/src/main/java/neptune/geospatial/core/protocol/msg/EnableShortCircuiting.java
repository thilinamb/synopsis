package neptune.geospatial.core.protocol.msg;

import ds.granules.communication.direct.control.ControlMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static neptune.geospatial.core.protocol.ProtocolTypes.ENABLE_SHORT_CIRCUITING;

/**
 * Protocol message to enable short circuiting at ingesters
 *
 * @author Thilina Buddhika
 */
public class EnableShortCircuiting extends ControlMessage {

    private String target;
    private String topic;
    private String streamType;
    private String fullStreamId;
    private String[] prefixList;

    public EnableShortCircuiting() {
        super(ENABLE_SHORT_CIRCUITING);
    }

    public EnableShortCircuiting(String target, String topic, String streamType, String fullStreamId, String[] prefixList) {
        super(ENABLE_SHORT_CIRCUITING);
        this.target = target;
        this.topic = topic;
        this.streamType = streamType;
        this.fullStreamId = fullStreamId;
        this.prefixList = prefixList;
    }

    @Override
    public void readValues(DataInputStream dataInputStream) throws IOException {
        this.target = dataInputStream.readUTF();
        this.topic = dataInputStream.readUTF();
        this.streamType = dataInputStream.readUTF();
        this.fullStreamId = dataInputStream.readUTF();
        int prefixListLength = dataInputStream.readInt();
        this.prefixList = new String[prefixListLength];
        for(int i = 0; i < prefixListLength; i++){
            this.prefixList[i] = dataInputStream.readUTF();
        }
    }

    @Override
    public void writeValues(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(this.target);
        dataOutputStream.writeUTF(this.topic);
        dataOutputStream.writeUTF(this.streamType);
        dataOutputStream.writeUTF(this.fullStreamId);
        dataOutputStream.writeInt(this.prefixList.length);
        for(int i = 0; i < this.prefixList.length; i++){
            dataOutputStream.writeUTF(this.prefixList[i]);
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getStreamType() {
        return streamType;
    }

    public String getFullStreamId() {
        return fullStreamId;
    }

    public String[] getPrefixList() {
        return prefixList;
    }

    public String getTarget() {
        return target;
    }
}
