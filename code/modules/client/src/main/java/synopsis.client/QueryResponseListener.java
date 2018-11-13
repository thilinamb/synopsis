package synopsis.client;

import ds.granules.communication.direct.control.ControlMessage;

public interface QueryResponseListener {
    public void handle(ControlMessage ctrlMsg);
}
