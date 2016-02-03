package neptune.geospatial.core.protocol;

import ds.funnel.topic.TopicDataEvent;
import ds.granules.communication.direct.control.ControlMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Parses each <code>TopicDataEvent</code> and returns an
 * instance of the appropriate control message.
 * This is a singleton class.
 *
 * @author Thilina Buddhika
 */
public class ProtocolFactory {

    private static ProtocolFactory instance = new ProtocolFactory();

    private ProtocolFactory(){
        // singleton, hence a private constructor
    }

    public static ProtocolFactory getInstance(){
        return instance;
    }

    public ControlMessage parse(TopicDataEvent topicDataEvent) throws ProtocolException {
        int messageType = getMessageType(topicDataEvent.getDataBytes());
        try {
            ControlMessage message = null;
            switch (messageType) {
                case Protocol:
                    break;
                default:
                    String errorMsg = "Unsupported message type: " + messageType;
                    throw new ProtocolException(errorMsg);
            }
            message.unmarshall(topicDataEvent.getDataBytes());
            return message;
        } catch (IOException e) {
            throw new ProtocolException(e.getMessage(), e);
        }
    }

    private int getMessageType(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        try {
            return dis.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bais.close();
                dis.close();
            } catch (IOException ignore) {
            }
        }
        return -1;
    }
}
