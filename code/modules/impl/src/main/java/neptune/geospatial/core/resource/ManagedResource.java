package neptune.geospatial.core.resource;

import ds.granules.Granules;
import ds.granules.communication.direct.dispatch.ControlMessageDispatcher;
import ds.granules.exception.CommunicationsException;
import ds.granules.exception.GranulesConfigurationException;
import ds.granules.neptune.interfere.core.NIException;
import ds.granules.neptune.interfere.core.NIUtil;
import ds.granules.scheduler.ExchangeProcessor;
import ds.granules.scheduler.Resource;
import ds.granules.util.Constants;
import ds.granules.util.NeptuneRuntime;
import ds.granules.util.ParamsReader;
import neptune.geospatial.core.AbstractProtocolHandler;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Improves the behavior of <code>Resource</code> by
 * adding support for control messages. This will allow
 * communication with the job deployer as well as other
 * Resources.
 *
 * @author Thilina Buddhika
 */
public class ManagedResource {

    private static Logger logger = Logger.getLogger(ManagedResource.class.getName());

    private static ManagedResource instance;
    private String controlEndpoint = null;
    private String dataEndpoint = null;
    private String deployerEndpoint = null;
    private ExchangeProcessor exchangeProcessor;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ManagedResource(Properties inProps, int numOfThreads) throws CommunicationsException {
        Resource resource = new Resource(inProps, numOfThreads);
        resource.init();
        logger.info("Successfully Started NIResource..");
    }

    private void init() {
        // register the callbacks to receive interference related control messages.
        instance = this;
        AbstractProtocolHandler protoHandler = new ResourceProtocolHandler(this);
        ControlMessageDispatcher.getInstance().registerCallback(Constants.WILD_CARD_CALLBACK, protoHandler);
        new Thread(protoHandler).start();
        try {
            dataEndpoint = NIUtil.getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(Constants.DIRECT_COMM_LISTENER_PORT);
            controlEndpoint = NIUtil.getHostInetAddress().getHostName() + ":" +
                    NeptuneRuntime.getInstance().getProperties().getProperty(Constants.DIRECT_COMM_CONTROL_PLANE_SERVER_PORT);
            deployerEndpoint = NeptuneRuntime.getInstance().getProperties().getProperty(
                    Constants.DEPLOYER_ENDPOINT);
            exchangeProcessor = Resource.getInstance().getExchangeProcessor();
            countDownLatch.await();
        } catch (GranulesConfigurationException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static ManagedResource getInstance() throws NIException {
        if (instance == null) {
            throw new NIException("NIResource is not initialized.");
        }
        return instance;
    }

    public static void main(String[] args) {
        ParamsReader paramsReader = Granules.getParamsReader();
        String configLocation = "conf/ResourceConfig.txt";

        if (args.length != 0) {
            configLocation = args[0];
        }

        try {
            Properties resourceProps = new Properties();

            /* Read properties from config file, if it exists. */
            File configFile = new File(configLocation);
            if (configFile.exists()) {
                resourceProps = paramsReader.getProperties(configLocation);
            }

            /* Use System properties, if available. These overwrite values
             * specified in the config file. */
            String[] propNames = new String[]{Constants.NUM_THREADS_ENV_VAR,
                    Constants.FUNNEL_BOOTSTRAP_ENV_VAR,
                    Constants.DIRECT_COMM_LISTENER_PORT,
                    Constants.IO_REACTOR_THREAD_COUNT,
                    Constants.DIRECT_COMM_LISTENER_PORT};

            for (String propName : propNames) {
                String propertyValue = System.getProperty(propName);

                if (propertyValue != null) {
                    resourceProps.setProperty(propName, propertyValue);
                }
            }

            NeptuneRuntime.initialize(resourceProps);

            int numOfThreads = Integer.parseInt(
                    resourceProps.getProperty(Constants.NUM_THREADS_ENV_VAR, "4"));

            ManagedResource resource = new ManagedResource(resourceProps, numOfThreads);
            resource.init();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    protected void acknowledgeCtrlMsgListenerStartup(){
        countDownLatch.countDown();
    }
}
