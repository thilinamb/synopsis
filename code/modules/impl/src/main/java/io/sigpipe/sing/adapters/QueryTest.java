package io.sigpipe.sing.adapters;

import io.sigpipe.sing.query.MetaQuery;
import io.sigpipe.sing.query.Query;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class QueryTest {

    public static void main(String[] args) throws Exception {

        QueryWrapper qw = QueryCreator.create();
        String q = getSQLFormat(qw);
        System.out.println(q);
    }

    public static String getSQLFormat(QueryWrapper qw) throws IOException, SerializationException {
        ByteArrayInputStream bIn = new ByteArrayInputStream(qw.payload);
        SerializationInputStream sIn = new SerializationInputStream(bIn);
        sIn.readByte();
        Query q = new MetaQuery(sIn);
        sIn.close();
        return q.toString();
    }
}
