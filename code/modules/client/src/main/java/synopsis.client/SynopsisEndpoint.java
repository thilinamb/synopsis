package synopsis.client;

/**
 * @author Thilina Buddhika
 */
class SynopsisEndpoint {

    private final String hostname;
    private final int controlPort;

    SynopsisEndpoint(String hostname, int controlPort) {
        this.hostname = hostname;
        this.controlPort = controlPort;
    }

    String getHostname() {
        return hostname;
    }

    int getControlPort() {
        return controlPort;
    }

    @Override
    public String toString() {
        return hostname + ":" + controlPort;
    }
}
