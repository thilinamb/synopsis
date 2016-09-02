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
    public static final int SCALE_OUT_COMPLETE = 12104;
    public static final int SCALE_OUT_COMPLETE_ACK = 12105;
    public static final int STATE_TRANSFER_COMPLETE_ACK = 12106;
    public static final int DEPLOYMENT_ACK = 12107;
    public static final int PREFIX_ONLY_SCALE_OUT_COMPLETE = 12108;
    public static final int ENABLE_SHORT_CIRCUITING = 12109;

    // scale in related protocol codes
    public static final int SCALE_IN_LOCK_REQ = 12201;
    public static final int SCALE_IN_LOCK_RESP = 12202;
    public static final int SCALE_IN_ACTIVATION_REQ = 12203;
    public static final int SCALE_IN_COMPLETE = 12204;
    public static final int SCALE_IN_COMPLETE_ACK = 12205;
    public static final int STATE_TRANSFER_MSG = 12300;
    // fault tolerance related protocol codes
    public static final int STATE_REPL_LEVEL_INCREASE = 12400;
    public static final int CHECKPOINT_ACK = 12401;
}
