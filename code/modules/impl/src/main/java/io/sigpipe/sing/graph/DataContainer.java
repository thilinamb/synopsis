package io.sigpipe.sing.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class DataContainer implements ByteSerializable {

    public List<ContainerEntry> entries;

    public DataContainer() {
        entries = new ArrayList<>();
    }

    public void merge(DataContainer container) {

    }

    public void clear() {

    }

    @Deserialize
    public DataContainer(SerializationInputStream in)
    throws IOException {
        int numEntries = in.readInt();
        this.entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; ++i) {
            ContainerEntry entry = new ContainerEntry(in);
            entries.add(entry);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(entries.size());
        for (ContainerEntry entry : entries) {
            entry.serialize(out);
        }
    }
}
