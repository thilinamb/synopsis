package synopsis.client.persistence;

import neptune.geospatial.core.protocol.msg.client.PersistStateAck;
import neptune.geospatial.core.protocol.msg.client.PersistStateResponse;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class PersistenceManager {

    private final Logger logger = Logger.getLogger(PersistenceManager.class);

    private static PersistenceManager instance;
    private Map<Long, OutstandingPersistenceTask> outstandingPersistenceTaskMap = new ConcurrentHashMap<>();
    private Map<Long, PersistenceCompletionCallback> callbacks = new ConcurrentHashMap<>();

    private PersistenceManager() {

    }

    public static PersistenceManager getInstance() {
        return instance;
    }

    public void submitPersistenceTask(long persistenceTaskId,
                                                   int nodeCount,
                                                   PersistenceCompletionCallback cb) {
        OutstandingPersistenceTask task = new OutstandingPersistenceTask(persistenceTaskId, nodeCount);
        outstandingPersistenceTaskMap.put(persistenceTaskId, task);
        callbacks.put(persistenceTaskId, cb);
    }

    public void handlePersistenceAck(PersistStateAck ack){
        long checkpointId = ack.getCheckpointId();
        boolean complete = outstandingPersistenceTaskMap.get(checkpointId).handlePersistStateAck(ack);
        if(complete){
            callbacks.remove(checkpointId).handlePersistenceCompletion(
                    outstandingPersistenceTaskMap.remove(checkpointId));
        }
    }

    public void handlePersistStateResponse(PersistStateResponse response){
        long checkpointId = response.getCheckpointId();
        boolean complete = outstandingPersistenceTaskMap.get(checkpointId).handlePersistStateResp(response);
        if(complete){
            callbacks.remove(checkpointId).handlePersistenceCompletion(
                    outstandingPersistenceTaskMap.remove(checkpointId));
        }
    }

}
