package neptune.geospatial.graph.operators;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.query.Expression;
import io.sigpipe.sing.query.Operator;
import io.sigpipe.sing.query.Query;
import io.sigpipe.sing.query.RelationalQuery;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.util.ReducedTestConfiguration;

public class QueryCreator {

    private static String[] geo2500km = { "8", "9", "b", "c", "d", "f" };

    private static String[] geo630km = {    "8g", "8u", "8v", "8x", "8y", "8z",
        "94", "95", "96", "97", "9d", "9e", "9g", "9h", "9j", "9k", "9m", "9n",
        "9p", "9q", "9r", "9s", "9t", "9u", "9v", "9w", "9x", "9y", "9z", "b8",
        "b9", "bb", "bc", "bf", "c0", "c1", "c2", "c3", "c4", "c6", "c8", "c9",
        "cb", "cc", "cd", "cf", "d4", "d5", "d6", "d7", "dd", "de", "dh", "dj",
        "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt", "dw", "dx", "dz", "f0",
        "f1", "f2", "f3", "f4", "f6", "f8", "f9", "fb", "fc", "fd", "ff"      };

    public enum QueryType {
        Relational,
        Metadata,
    }

    public enum SpatialScope {
        Geo2500km,
        Geo630km,
        Geo78km,
    }

    public static QueryWrapper create(QueryType type, SpatialScope scope) {
        Random random = new Random();

        String geohash = "";
        switch (scope) {
            case Geo2500km:
                geohash = geo2500km[random.nextInt(geo2500km.length)];
                break;

            case Geo630km:
                geohash = geo630km[random.nextInt(geo630km.length)];
                break;

            case Geo78km:
                //TODO finest granularity geohashes
                geohash = "";
                break;
        }

        /* Note: we're just using a RelationalQuery here to store the
         * expressions since Query is abstract (TODO: should Query really be
         * abstract, or should we provide another implementation for this use
         * case? */
        Query q = new RelationalQuery();

        q.addExpression(new Expression(
                    Operator.STR_PREFIX, new Feature("location", geohash)));

        /* Should set up ranges for all the features here: */
//        for (String feature : ReducedTestConfiguration.FEATURE_NAMES) {
//            q.addExpression(
//                    new Expression(
//                        Operator.RANGE_INC_EXC,
//                        new Feature(feature, 0.0f),
//                        new Feature(feature, 1.0f)));
//        }

        q.addExpression(
                new Expression(
                    Operator.GREATER,
                    new Feature("upward_short_wave_rad_flux_surface", 10.0f)));

        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        SerializationOutputStream sOut = new SerializationOutputStream(
                new BufferedOutputStream(bOut));

        try {
            sOut.writeByte((byte) type.ordinal());
            sOut.writeSerializable(q);
            sOut.close();
            bOut.close();
        } catch (IOException e) {
            System.out.println("Error serializing query!");
            e.printStackTrace();
        }

        QueryWrapper qw = new QueryWrapper();
        qw.payload = bOut.toByteArray();
        qw.geohashes.add(geohash);

        return qw;
    }
}
