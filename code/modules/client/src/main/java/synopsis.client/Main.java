package synopsis.client;

import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;
import org.apache.log4j.Logger;
import synopsis.client.persistence.JSONConfigPersistenceCallback;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryResponse;

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
            client.init();
            //testQuery(client);
            //testPersistState(client);
            testQClient(client);
            new CountDownLatch(1).await();
        } catch (IOException e) {
            LOGGER.error("Error when populating the properties.", e);
        } catch (ClientException e) {
            LOGGER.error("Error initializing Synopsis client.", e);
        } catch (InterruptedException ignore) {

        }
    }

    private static void testQuery(Client client) throws ClientException {
        QueryWrapper qw = QueryCreator.create(QueryCreator.QueryType.Relational, QueryCreator.SpatialScope.Geo630km);
        client.submitQuery(qw.payload, qw.geohashes, new QueryCallback() {
            @Override
            public void processQueryResponse(QueryResponse response) {
                System.out.println("RECEIVED A QUERY RESPONSE! ELAPSED TIME: " + response.getElapsedTimeInMS());
            }
        });
    }

    private static void testPersistState(Client client) throws ClientException {
        client.serializeState(new JSONConfigPersistenceCallback());
    }

    private static void testQClient(Client client) throws ClientException{
        QueryWrapper[] queries = new QueryWrapper[6];
        queries[0] = QueryCreator.create(QueryCreator.QueryType.Relational, QueryCreator.SpatialScope.Geo78km);
        queries[1] = QueryCreator.create(QueryCreator.QueryType.Relational, QueryCreator.SpatialScope.Geo630km);
        queries[2] = QueryCreator.create(QueryCreator.QueryType.Relational, QueryCreator.SpatialScope.Geo2500km);
        queries[3] = QueryCreator.create(QueryCreator.QueryType.Metadata, QueryCreator.SpatialScope.Geo78km);
        queries[4] = QueryCreator.create(QueryCreator.QueryType.Metadata, QueryCreator.SpatialScope.Geo630km);
        queries[5] = QueryCreator.create(QueryCreator.QueryType.Metadata, QueryCreator.SpatialScope.Geo2500km);
        // query types
        QueryCreator.QueryType[] qTypes = new QueryCreator.QueryType[6];
        qTypes[0] = QueryCreator.QueryType.Relational;
        qTypes[1] = QueryCreator.QueryType.Relational;
        qTypes[2] = QueryCreator.QueryType.Relational;
        qTypes[3] = QueryCreator.QueryType.Metadata;
        qTypes[4] = QueryCreator.QueryType.Metadata;
        qTypes[5] = QueryCreator.QueryType.Metadata;
        double[] percentages = new double[]{0.1d, 0.1d, 0.1d, 0.3d, 0.3d, 0.1d};
        client.dispatchQClients(10, 100, queries, qTypes, percentages);
    }
}

/**
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9q] New prefix 9q added to 2bbe4594-d6f0-4214-9a9a-484ba588777e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9w] New prefix 9w added to 0a2bad95-389b-444e-9085-ec61fe29c7a6
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [c1] New prefix c1 added to bc094903-21f7-4b9e-8b88-df22e648f6b2
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9u] New prefix 9u added to 90370029-66d7-42ca-81a8-013bc22b8c8e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [dt] New prefix dt added to 2bbe4594-d6f0-4214-9a9a-484ba588777e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [f9] New prefix f9 added to 90370029-66d7-42ca-81a8-013bc22b8c8e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9e] New prefix 9e added to 0a2bad95-389b-444e-9085-ec61fe29c7a6
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [cc] New prefix cc added to bc094903-21f7-4b9e-8b88-df22e648f6b2
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9h] New prefix 9h added to 90370029-66d7-42ca-81a8-013bc22b8c8e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [cc] New prefix cc added to bc094903-21f7-4b9e-8b88-df22e648f6b2
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9x] New prefix 9x added to 2bbe4594-d6f0-4214-9a9a-484ba588777e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [dx] New prefix dx added to 0a2bad95-389b-444e-9085-ec61fe29c7a6
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [c1] New prefix c1 added to bc094903-21f7-4b9e-8b88-df22e648f6b2
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [c8] New prefix c8 added to 90370029-66d7-42ca-81a8-013bc22b8c8e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [c3] New prefix c3 added to 2bbe4594-d6f0-4214-9a9a-484ba588777e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [bb] New prefix bb added to 90370029-66d7-42ca-81a8-013bc22b8c8e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [c4] New prefix c4 added to 2bbe4594-d6f0-4214-9a9a-484ba588777e
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9m] New prefix 9m added to bc094903-21f7-4b9e-8b88-df22e648f6b2
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [dx] New prefix dx added to 0a2bad95-389b-444e-9085-ec61fe29c7a6
 * lattice-35 | 2016-10-13 23:33:01 INFO  Node:69 - [9p] New prefix 9p added to 90370029-66d7-42ca-81a8-013bc22b8c8e
 */
