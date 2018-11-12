package synopsis.samples.sketch;

import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;
import synopsis.client.Client;
import synopsis.client.ClientException;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryResponse;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class QueryClient extends Client {

    public QueryClient(Properties properties, int clientPort) throws ClientException {
        super(properties, clientPort);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: <path-to-config>");
            System.exit(-1);
        }
        String configFilePath = args[0];
        int port = 8891;
        System.out.println("Using the config file: " + configFilePath);
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFilePath));
            QueryClient client = new QueryClient(properties, port);
            client.init();
            client.queryManager.setDispatcherModeEnabled(false);    // enables callbacks
            QueryWrapper qw = QueryCreator.create(QueryCreator.QueryType.Relational, QueryCreator.SpatialScope.Geo630km);
            client.submitQuery(01l, qw.payload, qw.geohashes, new QueryCallback() {
                @Override
                public void processQueryResponse(QueryResponse response) {
                    System.out.println("RECEIVED A QUERY RESPONSE!. ELAPSED TIME (ns): " + response.getElapsedTimeInNanoS());
                    // take a look at the list of byte[] inside the QueryResponse (variable name "queryResponse") for responses from individual sketchlets that contained data.
                }
            });
        } catch (IOException | ClientException e) {
            e.printStackTrace();
        }
    }
}
