package neptune.geospatial.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Thilina Buddhika
 */
public class RivuletUtil {

    public static InetAddress getHostInetAddress() {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        return inetAddr;
    }
}
