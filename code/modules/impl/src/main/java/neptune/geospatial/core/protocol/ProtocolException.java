package neptune.geospatial.core.protocol;

/**
 * Exception related to protocol handling.
 *
 * @author Thilina Buddhika
 */
public class ProtocolException extends Throwable{
    public ProtocolException() {
    }

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtocolException(Throwable cause) {
        super(cause);
    }
}
