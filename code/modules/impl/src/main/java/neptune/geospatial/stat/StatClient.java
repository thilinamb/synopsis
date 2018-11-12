package neptune.geospatial.stat;

import ds.granules.communication.direct.control.SendUtility;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.util.NeptuneRuntime;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * A simple client to publish statistics more conveniently.
 *
 * @author Thilina Buddhika
 */
public class StatClient {

    private static final String STAT_SERVER_ENDPOINT = "stat-server-endpoint";

    private final Logger logger = Logger.getLogger(StatClient.class);
    private static StatClient instance;
    private String statServerEndpoint;

    private StatClient(){
        try {
            statServerEndpoint = NeptuneRuntime.getInstance().getProperties().getProperty(STAT_SERVER_ENDPOINT);
        } catch (GranulesConfigurationException e) {
           logger.error("Error loading startup properties.", e);
        }
    }

    public synchronized static StatClient getInstance(){
        if(instance == null){
            instance = new StatClient();
        }
        return instance;
    }

    public void publish(StatisticsRecord statRecord){
        if (statServerEndpoint != null) {
            try {
                SendUtility.sendControlMessage(statServerEndpoint, statRecord);
                if(logger.isTraceEnabled()) {
                    logger.trace("Published a message of type: " + statRecord.getClass().getName() + "to " + statServerEndpoint);
                }
            } catch (CommunicationsException | IOException e) {
                logger.error("Error publising stats.", e);
            }
        } else {
            logger.error("Stat Server endpoint is not set.");
        }
    }
}
