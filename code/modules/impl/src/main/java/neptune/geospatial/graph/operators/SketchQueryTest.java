package neptune.geospatial.graph.operators;

import io.sigpipe.sing.adapters.ReadMetaBlob;

public class SketchQueryTest {

    public static void main(String[] args)
    throws Exception {
        SketchProcessor sp = new SketchProcessor();
        ReadMetaBlob.init();

        /* Add data to the sketch */
        for (String file : args) {
            ReadMetaBlob.loadData(file, sp.sketch);
        }

        QueryWrapper q1 = QueryCreator.create(
                QueryCreator.QueryType.Relational,
                QueryCreator.SpatialScope.Geo630km);

        QueryWrapper q2 = QueryCreator.create(
                QueryCreator.QueryType.Metadata,
                QueryCreator.SpatialScope.Geo2500km);

        byte[] result1 = sp.query(q1.payload);
        byte[] result2 = sp.query(q2.payload);

        System.out.println("Result sizes: "
                + result1.length + ", "
                + result2.length);
    }

}
