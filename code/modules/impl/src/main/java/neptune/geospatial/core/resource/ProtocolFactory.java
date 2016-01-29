package neptune.geospatial.core.resource;

import ds.funnel.topic.TopicDataEvent;
import ds.granules.communication.direct.control.ControlMessage;

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
        //
    }

    public static ProtocolFactory getInstance(){
        return instance;
    }

    public ControlMessage parse(TopicDataEvent topicDataEvent){
        return null;
    }
}
