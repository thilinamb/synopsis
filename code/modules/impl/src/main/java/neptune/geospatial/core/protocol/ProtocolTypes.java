package neptune.geospatial.core.protocol;

/**
 * @author Thilina Buddhika
 */
public final class ProtocolTypes {
    // scale out related protocol codes
    public static final int SCALE_OUT_REQ = 12100;
    public static final int SCALE_OUT_RESP = 12101;
    public static final int SCALE_OUT_LOCK_REQ = 12102;
    public static final int SCALE_OUT_LOCK_RESP = 12103;
    public static final int SCALE_OUT_COMPLETE_ACK = 12104;
    public static final int DEPLOYMENT_ACK = 12105;
    // scale in related protocol code
    public static final int SCALE_IN_LOCK_REQ = 12201;
    public static final int SCALE_IN_LOCK_RESP = 12202;
    public static final int SCALE_IN_ACTIVATION_REQ = 12203;
    public static final int SCALE_IN_COMPLETE = 12204;
    public static final int SCALE_IN_COMPLETE_ACK = 12205;
    public static final int STATE_TRANSFER_MSG = 12300;
}
