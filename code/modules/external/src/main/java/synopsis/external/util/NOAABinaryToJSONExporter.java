package synopsis.external.util;

import com.google.gson.Gson;
import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.feature.FeatureSet;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.Serializer;
import io.sigpipe.sing.util.ReducedTestConfiguration;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class NOAABinaryToJSONExporter {
    public static void main(String[] args) {
        try {

            List<String> featuresOfInterest = Arrays.asList(ReducedTestConfiguration.FEATURE_NAMES);

            String fileName = "/Users/thilina/Desktop/namanl_218_20140113_0000_002.grb.mblob";
            FileInputStream fIn = new FileInputStream(fileName);
            BufferedInputStream bIn = new BufferedInputStream(fIn);
            SerializationInputStream inStream = new SerializationInputStream(bIn);
            int totalMessagesInCurrentFile = inStream.readInt();
            System.out.println("Total message count: " + totalMessagesInCurrentFile);
            Gson gson = new Gson();
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/users/thilina/Desktop/exported.json"));
            for (int i = 0; i < totalMessagesInCurrentFile; i++) {
                float lat = inStream.readFloat();
                float lon = inStream.readFloat();
                byte[] payload = inStream.readField();
                Metadata eventMetadata = Serializer.deserialize(Metadata.class, payload);
                FeatureSet featureSet = eventMetadata.getAttributes();
                Map<String, Double> extractedData = new HashMap<>();
                for(String f : featuresOfInterest){
                    extractedData.put(f, featureSet.get(f).getDouble());
                }
                bufferedWriter.write(gson.toJson(extractedData));
            }
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException | SerializationException e) {
            e.printStackTrace();
        }
    }
}
