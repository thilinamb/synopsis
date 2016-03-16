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

    public static InetAddress getHostInetAddress() {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        return inetAddr;
    }

    public static String getCtrlEndpoint() throws GranulesConfigurationException {
        try {
            return RivuletUtil.getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(
                            Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT);
        } catch (GranulesConfigurationException e) {
            logger.error("Error when retrieving the hostname.", e);
            throw e;
        }
    }
}
