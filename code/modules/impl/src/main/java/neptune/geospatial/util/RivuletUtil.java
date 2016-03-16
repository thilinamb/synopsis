package neptune.geospatial.util;

import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Thilina Buddhika
 */
public class RivuletUtil {

    private static final Logger logger = Logger.getLogger(RivuletUtil.class);

    /**
     * Get the {@code InetAddress} of the current host
     * @return @code InetAddress} of the current host
     */
    public static InetAddress getHostInetAddress() {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        return inetAddr;
    }

    /**
     * Returns the control plain address of the current process
     * @return Control plain address
     * @throws GranulesConfigurationException Error reading the configuration file
     */
    public static String getCtrlEndpoint() throws GranulesConfigurationException {
        try {
            return getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(
                            Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT);
        } catch (GranulesConfigurationException e) {
            logger.error("Error when retrieving the hostname.", e);
            throw e;
        }
    }
}
