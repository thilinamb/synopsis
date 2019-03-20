package synopsis.samples.sketch;

import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.graph.DataContainer;
import io.sigpipe.sing.query.*;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;
import synopsis.client.Client;
import synopsis.client.ClientException;
import synopsis.client.query.QueryCallback;
import synopsis.client.query.QueryResponse;

import java.io.*;
import java.util.Properties;

public class QueryClient extends Client {

    private QueryClient(Properties properties, int clientPort) throws ClientException {
        super(properties, clientPort);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: <path-to-config>");
            System.exit(-1);
        }
        String configFilePath = args[0];
        int port = 8897;    // if you execute the client over and over again with the same port without restarting the cluster, only the first request will work.
        // Granules' network implementation does not retire older connections --- hence retaining expired sockets.
        System.out.println("Using the config file: " + configFilePath);
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFilePath));
            QueryClient client = new QueryClient(properties, port);
            client.init();
            client.queryManager.setDispatcherModeEnabled(false);    // enables callbacks
            //QueryWrapper qw = QueryCreator.create(QueryCreator.QueryType.Relational, QueryCreator.SpatialScope.Geo78km);
            QueryWrapper mqw = getMQWrapper();
            client.submitQuery(3L, mqw.payload, mqw.geohashes, new QueryCallback() {
                @Override
                public void processQueryResponse(QueryResponse response) {
                    byte[] bytes = response.getQueryResponse().get(0);
                    System.out.println("RECEIVED A QUERY RESPONSE!. Id: " + response.getQueryId() + ". ELAPSED TIME (ns): " +
                            response.getElapsedTimeInNanoS() + ", size: " + bytes.length);
                    // take a look at the list of byte[] inside the QueryResponse (variable name "queryResponse") for responses from individual sketchlets that contained data.
                    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                    BufferedInputStream buffIn = new BufferedInputStream(byteIn);

                    SerializationInputStream serialIn =
                            new SerializationInputStream(buffIn);
                    try {
                        DataContainer dc = new DataContainer(serialIn);
                        System.out.println("Rec. Count: " + dc.statistics.count());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            QueryWrapper rqw = getRQWrapper();
            client.submitQuery(4L, rqw.payload, rqw.geohashes, new QueryCallback() {
                @Override
                public void processQueryResponse(QueryResponse response) {
                    byte[] bytes = response.getQueryResponse().get(0);
                    System.out.println("RECEIVED A QUERY RESPONSE!. Id: " + response.getQueryId() + ". ELAPSED TIME (ns): " +
                            response.getElapsedTimeInNanoS() + ", size: " + bytes.length);
                }
            });
        } catch (IOException | ClientException e) {
            e.printStackTrace();
        }
    }

    private static QueryWrapper getMQWrapper(){
        MetaQuery mq = new MetaQuery();
        mq.addExpression(new Expression(
                Operator.STR_PREFIX, new Feature("location", "9x")));
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        SerializationOutputStream sOut = new SerializationOutputStream(
                new BufferedOutputStream(bOut));

        try {
            sOut.writeByte((byte) QueryCreator.QueryType.Metadata.ordinal());
            sOut.writeSerializable(mq);
            sOut.close();
            bOut.close();
        } catch (IOException e) {
            System.out.println("Error serializing query!");
            e.printStackTrace();
        }

        QueryWrapper qw = new QueryWrapper();
        qw.payload = bOut.toByteArray();
        qw.geohashes.add("9x");
        return qw;
    }

    private static QueryWrapper getRQWrapper(){
        RelationalQuery rq = new RelationalQuery();
        rq.addExpression(new Expression(
                Operator.STR_PREFIX, new Feature("location", "9x")));
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        SerializationOutputStream sOut = new SerializationOutputStream(
                new BufferedOutputStream(bOut));

        try {
            sOut.writeByte((byte) QueryCreator.QueryType.Relational.ordinal());
            sOut.writeSerializable(rq);
            sOut.close();
            bOut.close();
        } catch (IOException e) {
            System.out.println("Error serializing query!");
            e.printStackTrace();
        }

        QueryWrapper qw = new QueryWrapper();
        qw.payload = bOut.toByteArray();
        qw.geohashes.add("9x");
        return qw;
    }
}
