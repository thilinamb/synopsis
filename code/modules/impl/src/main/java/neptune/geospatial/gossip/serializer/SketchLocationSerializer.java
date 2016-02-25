package neptune.geospatial.gossip.serializer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import neptune.geospatial.gossip.type.SketchLocation;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class SketchLocationSerializer implements StreamSerializer<SketchLocation> {

    @Override
    public void write(ObjectDataOutput objectDataOutput, SketchLocation sketchLocation) throws IOException {
        objectDataOutput.writeUTF(sketchLocation.getComputation());
        objectDataOutput.writeUTF(sketchLocation.getCtrlEndpoint());
    }

    @Override
    public SketchLocation read(ObjectDataInput objectDataInput) throws IOException {
        String computation = objectDataInput.readUTF();
        String ctrlEndpoint = objectDataInput.readUTF();
        return new SketchLocation(computation, ctrlEndpoint);
    }

    @Override
    public int getTypeId() {
        return 13341;
    }

    @Override
    public void destroy() {

    }
}
