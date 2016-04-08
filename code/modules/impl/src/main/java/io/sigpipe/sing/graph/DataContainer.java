package io.sigpipe.sing.graph;

import java.io.IOException;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.stat.RunningStatisticsND;

public class DataContainer implements ByteSerializable {

    public RunningStatisticsND statistics;

    public DataContainer() {
        this.statistics = new RunningStatisticsND();
    }

    public DataContainer(RunningStatisticsND statistics) {
        this.statistics = statistics;
    }

    public void merge(DataContainer container) {
        statistics.merge(container.statistics);
    }

    public void clear() {
        statistics.clear();
    }

    @Deserialize
    public DataContainer(SerializationInputStream in)
    throws IOException {
        statistics = new RunningStatisticsND(in);
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        statistics.serialize(out);
    }
}
