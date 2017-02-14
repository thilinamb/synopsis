package synopsis.client.failuredetection;

import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import neptune.geospatial.core.protocol.msg.client.TerminateNode;
import neptune.geospatial.ft.zk.MembershipChangeListener;
import org.apache.log4j.Logger;
import synopsis.client.SynopsisEndpoint;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Triggers failures and tracks time elapsed when the notification is received.
 * Used to estimate recovery time in case of a node failure
 *
 * @author Thilina Buddhika
 */
public class FailureMonitor implements MembershipChangeListener {

    private final Logger logger = Logger.getLogger(FailureMonitor.class);
    private final List<SynopsisEndpoint> endpoints;
    private final List<String> completed = new ArrayList<>();
    private BufferedWriter bfw;

    public FailureMonitor(List<SynopsisEndpoint> endpoints) throws IOException {
        this.endpoints = endpoints;
        try {
            bfw = new BufferedWriter(new FileWriter("/tmp/notification_time.csv"));
        } catch (IOException e) {
            logger.error("Error creating file to record elapsed time.", e);
            throw e;
        }
    }

    public void triggerFailures() {
        for (SynopsisEndpoint ep : endpoints) {
            // send a message asking node to exit
            String epAddr = ep.getHostname() + ":" + ep.getDataPort();
            String epCtrlAddr = ep.getHostname() + ":" + ep.getControlPort();
            logger.error("Terminating the node: " + epAddr);
            try {
                TerminateNode terminateNode = new TerminateNode();
                synchronized (completed) {
                    long t1 = System.currentTimeMillis();
                    SendUtility.sendControlMessage(epCtrlAddr, terminateNode);
                    // wait for notification from ZK
                    if (completed.isEmpty()) {
                        try {
                            completed.wait();
                        } catch (InterruptedException e) {
                            logger.error("Interrupted.", e);
                        }
                    }
                    String terminated = completed.get(0);
                    if (terminated.equals(epAddr)) {
                        long t2 = System.currentTimeMillis();
                        // track time elapsed
                        writeOutput(t2 - t1);
                        completed.remove(0);
                        logger.info("Termination is complete for " + epAddr + ", Time elapsed to get the notification: " + (t2 - t1));
                    } else {
                        logger.warn("Mismatching endpoints. Expected: " + epAddr + ", Received: " + terminated);
                    }
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error sending the terminate request to " + epAddr);
            }
        }
        // finalize output
        try {
            bfw.flush();
            bfw.close();
        } catch (IOException e) {
            logger.error("Error closing output streams.", e);
        }
    }

    @Override
    public void membershipChanged(List<String> lostMembers) {
        logger.info("Membership change detected. Member count: " + lostMembers.size() + ", Member: " +
                lostMembers.get(0));
        synchronized (completed) {
            completed.add(lostMembers.get(0));
            completed.notifyAll();
        }
    }

    private void writeOutput(long dt) throws IOException {
        bfw.write(dt + "\n");
        bfw.flush();
    }
}
