package synopsis.client;

/**
 * @author Thilina Buddhika
 */
public class SynopsisEndpoint {

    private final String hostname;
    private final int dataPort;
    private final int controlPort;

    SynopsisEndpoint(String hostname, int dataPort, int controlPort) {
        this.hostname = hostname;
        this.dataPort = dataPort;
        this.controlPort = controlPort;
    }

    public String getHostname() {
        return hostname;
    }

    public int getControlPort() {
        return controlPort;
    }

    public int getDataPort() {
        return dataPort;
    }

    @Override
    public String toString() {
        return hostname + ":" + dataPort + ":" + controlPort;
    }
}
