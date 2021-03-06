package neptune.geospatial.core.protocol;

import ds.funnel.topic.TopicDataEvent;
import ds.granules.communication.direct.ChannelReaderCallback;
import ds.granules.communication.direct.control.ControlMessage;
import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Listens for incoming messages through the control channel and outsource
 * the handling of specific messages to the concrete implementations
 * of this class.
 *
 * @author Thilina Buddhika
 */
public abstract class AbstractProtocolHandler implements ChannelReaderCallback, Runnable {

    private final Queue<TopicDataEvent> controlMessageQueue = new ConcurrentLinkedDeque<>();
    private ProtocolFactory protocolFactory = ProtocolFactory.getInstance();
    private boolean firstIteration = true;
    private Logger logger = Logger.getLogger(AbstractProtocolHandler.class);

    @Override
    public void onEvent(TopicDataEvent topicDataEvent) {
        synchronized (controlMessageQueue) {
            controlMessageQueue.add(topicDataEvent);
            controlMessageQueue.notifyAll();
        }
    }

    @Override
    public void run() {
        // notify the host instance of the ProtocolHandler the proper start of the
        // protocol handling thread.
        if (firstIteration) {
            notifyStartup();
            firstIteration = false;
        }
        // start listening to the control messages.
        while (!Thread.interrupted()) {
            try {
                synchronized (controlMessageQueue) {
                    if (controlMessageQueue.size() == 0) {
                        try {
                            controlMessageQueue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                if(controlMessageQueue.size() > 0) {
                    TopicDataEvent topicDataEvent = controlMessageQueue.remove();
                    ControlMessage ctrlMsg = protocolFactory.parse(topicDataEvent);
                    handle(ctrlMsg);
                }
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);    // log and continue
            }
        }
    }

    public abstract void handle(ControlMessage ctrlMsg);

    public abstract void notifyStartup();
}
