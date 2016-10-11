package synopsis.client;

/**
 * @author Thilina Buddhika
 */
public class SynopsisEndpoint {

    private final String hostname;
    private final int controlPort;

    public SynopsisEndpoint(String hostname, int controlPort) {
        this.hostname = hostname;
        this.controlPort = controlPort;
    }

    public String getHostname() {
        return hostname;
    }

    public int getControlPort() {
        return controlPort;
    }
}
