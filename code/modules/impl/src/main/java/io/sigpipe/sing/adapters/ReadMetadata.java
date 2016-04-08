package io.sigpipe.sing.adapters;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.SpatialProperties;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.Serializer;

public class ReadMetadata {

    public static List<Metadata> readMetaBlob(File file)
    throws FileNotFoundException, IOException, SerializationException {
        List<Metadata> metadataList = new ArrayList<>();

        FileInputStream fIn = new FileInputStream(file);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        SerializationInputStream in = new SerializationInputStream(bIn);

        int num = in.readInt();
        for (int i = 0; i < num; ++i) {
            float lat = in.readFloat();
            float lon = in.readFloat();
            byte[] payload = in.readField();

            Metadata m = Serializer.deserialize(Metadata.class, payload);
            m.setSpatialProperties(new SpatialProperties(lat, lon));
            metadataList.add(m);
        }

        in.close();

        return metadataList;
    }
}
