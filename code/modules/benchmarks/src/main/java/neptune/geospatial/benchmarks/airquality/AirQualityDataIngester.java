package neptune.geospatial.benchmarks.airquality;

import ds.granules.streaming.core.exception.StreamingDatasetException;
import io.sigpipe.sing.dataset.Metadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.Serializer;
import neptune.geospatial.graph.messages.GeoHashIndexedRecord;
import neptune.geospatial.graph.operators.NOAADataIngester;
import neptune.geospatial.util.RivuletUtil;
import neptune.geospatial.util.geohash.GeoHash;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;

/**
 * @author Thilina Buddhika
 */
public class AirQualityDataIngester extends NOAADataIngester {

    private Logger logger = Logger.getLogger(AirQualityDataIngester.class);
    private BigInteger bigInt1 = new BigInteger(128, new Random());
    private BigInteger bigInt2 = new BigInteger(128, new Random());

    @Override
    protected String getRootDataDirPath() {
        ///s/lattice-95/d/nobackup/granules/air_quality_data/csv/
        String hostname = RivuletUtil.getHostInetAddress().getHostName();
        return "/s/" + hostname + "/d/nobackup/granules/air_quality_data/csv/";
    }

    @Override
    protected GeoHashIndexedRecord nextRecord() {
        if (indexLastReadFile == 0 && totalMessagesInCurrentFile == 0) { // reading the very first record
            try {
                Thread.sleep(this.initialWaitPeriodMS);
            } catch (InterruptedException ignore) {

            }
            logger.info("Initial wait is complete. Starting the ingestion!");
            startNextFile();
            return parse();
        }
        else if (countEmittedFromCurrentFile < totalMessagesInCurrentFile) { // in the middle of a file
            return parse();
        } else if (indexLastReadFile < inputFiles.length && totalMessagesInCurrentFile == countEmittedFromCurrentFile) { // start next file.
            startNextFile();
            logger.info(String.format("Reading file: %d of %d", indexLastReadFile, inputFiles.length));
            return parse();
        } else if (indexLastReadFile == inputFiles.length) {
            logger.info("Finished reading entire input.");
            return null;
        }
        return null;
    }

    @Override
    public void emit() throws StreamingDatasetException {
        try {
            super.emit();
            bigInt1.isProbablePrime(50);
        } catch (Throwable e){
            logger.error("Error in emitting data.", e);
        }
    }

    // some test code to see if exported files have the expected attributes
    public static void main(String[] args) {
        try {
            SerializationInputStream inStream = new SerializationInputStream(new FileInputStream("/tmp/hourly_42101_2016.csv.mblob"));
            int recCount = inStream.readInt();
            for (int i = 0; i < 2; i++) {
                float lat = inStream.readFloat();
                float lon = inStream.readFloat();
                byte[] payload = inStream.readField();
                String stringHash = GeoHash.encode(lat, lon, 3);
                Metadata eventMetadata = Serializer.deserialize(Metadata.class, payload);
                System.out.println("Geohash = " + stringHash + ", ts = " + eventMetadata.getTemporalProperties().getStart());
                for (Feature f : eventMetadata.getAttributes()) {
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
