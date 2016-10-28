package synopsis.client.persistence;

import ds.funnel.data.format.FormatReader;
import neptune.geospatial.graph.operators.NOAADataIngester;

/**
 * A hack to get data ingester to start from half way of the dataset.
 * This works with loading persisted state from disk to cut down the benchmark time.
 *
 * @author Thilina Buddhika
 */
public class PartialDatasetIngester extends NOAADataIngester{

    @Override
    protected void deserializeMemberVariables(FormatReader formatReader) {
        super.deserializeMemberVariables(formatReader);
        this.initialWaitPeriodMS = 300 * 60 * 1000;
        this.years = new String[]{"2015"};
    }
}
