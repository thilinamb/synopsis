package io.sigpipe.sing.graph;

import java.io.IOException;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.stat.RunningStatistics2D;

public class ContainerEntry implements ByteSerializable {
    public int feature1ID;
    public int feature2ID;
    public RunningStatistics2D stats;

    public ContainerEntry() {

    }

    @Deserialize
    public ContainerEntry(SerializationInputStream in)
    throws IOException {
        this.feature1ID = in.readInt();
        this.feature2ID = in.readInt();
        stats = new RunningStatistics2D(in);
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(feature1ID);
        out.writeInt(feature2ID);
        stats.serialize(out);
    }

}

