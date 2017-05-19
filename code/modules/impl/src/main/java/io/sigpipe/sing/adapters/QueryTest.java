package io.sigpipe.sing.adapters;

import java.io.ByteArrayInputStream;

import io.sigpipe.sing.query.MetaQuery;
import io.sigpipe.sing.query.Query;
import io.sigpipe.sing.serialization.SerializationInputStream;

import neptune.geospatial.graph.operators.QueryCreator;
import neptune.geospatial.graph.operators.QueryWrapper;

public class QueryTest {

    public static void main(String[] args) throws Exception {

        QueryWrapper qw = QueryCreator.create();
        ByteArrayInputStream bIn = new ByteArrayInputStream(qw.payload);
        SerializationInputStream sIn = new SerializationInputStream(bIn);
        sIn.readByte();
        Query q = new MetaQuery(sIn);
        System.out.println(q);
        sIn.close();

    }
}
