package io.sigpipe.sing.graph;

import java.io.IOException;

import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class CountContainer extends DataContainer {

    public long a;
    public long b;

    public CountContainer() {

    }

    public CountContainer(long a, long b) {
        this.a = a;
        this.b = b;
    }

    public void merge(DataContainer container) {
        CountContainer cc = (CountContainer) container;
        this.a += cc.a;
        this.b += cc.b;
    }

    public void clear() {
        this.a = 0;
        this.b = 0;
    }

    @Deserialize
    public CountContainer(SerializationInputStream in)
    throws IOException {
        this.a = in.readLong();
        this.b = in.readLong();
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeLong(a);
        out.writeLong(b);
    }

}
