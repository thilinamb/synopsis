package neptune.geospatial.benchmarks.airquality;

import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.Serializer;
import neptune.geospatial.graph.operators.NOAADataIngester;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.geohash.GeoHash;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class AirQualityDataIngester extends NOAADataIngester{

    @Override
    protected String getRootDataDirPath() {
        ///s/lattice-95/d/nobackup/granules/air_quality_data/csv/
        String hostname = RivuletUtil.getHostInetAddress().getHostName();
        return "/s/" + hostname + "/d/nobackup/granules/air_quality_data/csv/";
    }

    // some test code to see if exported files have the expected attributes
    public static void main(String[] args) {
        try {
            SerializationInputStream inStream = new SerializationInputStream(new FileInputStream("/tmp/hourly_42101_2016.csv.mblob"));
            int recCount = inStream.readInt();
            for(int i = 0; i < 2; i++){
                float lat = inStream.readFloat();
                float lon = inStream.readFloat();
                byte[] payload = inStream.readField();
                String stringHash = GeoHash.encode(lat, lon, 3);
                Metadata eventMetadata = Serializer.deserialize(Metadata.class, payload);
                System.out.println("Geohash = " + stringHash + ", ts = " + eventMetadata.getTemporalProperties().getStart());
                for(Feature f : eventMetadata.getAttributes()){
                    System.out.println(f.getName() + "-> " + f.dataToString());
                }
                System.out.println("------------");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SerializationException e) {
            e.printStackTrace();
        }
    }
}
