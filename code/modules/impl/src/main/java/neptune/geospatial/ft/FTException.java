package neptune.geospatial.ft;

/**
 * Fault tolerance related errors
 *
 * @author Thilina Buddhika
 */
public class FTException extends Exception {
    public FTException() {
    }

    public FTException(String message) {
        super(message);
    }

    public FTException(String message, Throwable cause) {
        super(message, cause);
    }
}
