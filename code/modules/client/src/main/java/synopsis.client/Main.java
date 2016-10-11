package synopsis.client;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Main class of the Synopsis Client
 *
 * @author Thilina Buddhika
 */
public class Main {

    private static Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.err.println("Usage: <path-to-config> <port>");
                System.exit(-1);
            }
            String configFilePath = args[0];
            int port = Integer.parseInt(args[1]);
            LOGGER.info("Using the config file: " + configFilePath);
            LOGGER.info("Using the client port: " + port);
            Properties properties = new Properties();
            properties.load(new FileInputStream(configFilePath));
            Client client = new Client(properties, port);
            new CountDownLatch(1).await();
        } catch (IOException e) {
            LOGGER.error("Error when populating the properties.", e);
        } catch (ClientException e) {
            LOGGER.error("Error initializing Synopsis client.", e);
        } catch (InterruptedException ignore) {

        }
    }
}
