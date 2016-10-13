package synopsis.client;

/**
 * @author Thilina Buddhika
 */
public class ClientException extends Throwable {

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
