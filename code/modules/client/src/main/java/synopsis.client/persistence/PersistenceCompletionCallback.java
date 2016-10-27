package synopsis.client.persistence;

/**
 * @author Thilina Buddhika
 */
public interface PersistenceCompletionCallback {

    public void handlePersistenceCompletion(OutstandingPersistenceTask task);

    public boolean isCompleted();
}
