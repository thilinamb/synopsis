package io.sigpipe.sing.dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.sigpipe.sing.adapters.ReadMetadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.stat.RunningStatistics;
import io.sigpipe.sing.stat.SquaredError;
import io.sigpipe.sing.util.TestConfiguration;

public class TickEvaluator {

    private Quantizer quantizer;
    private Map<Feature, RunningStatistics> statMap = new HashMap<>();

    public TickEvaluator(Quantizer quantizer) {
        this.quantizer = quantizer;
    }

    public void train(List<Feature> features) {
        for (Feature feature : features) {
            Feature q = quantizer.quantize(feature);
            RunningStatistics rs = statMap.get(q);
            if (rs == null) {
                rs = new RunningStatistics();
                statMap.put(q, rs);
            }
            rs.put(feature.getDouble());
        }
    }

    public void evaluate(List<Feature> features) {
        RunningStatistics allStats = new RunningStatistics();

        Map<Feature, SquaredError> errors = new HashMap<>();
        for (Feature feature : features) {
            allStats.put(feature.getDouble());

            Feature q = quantizer.quantize(feature);
            RunningStatistics rs = statMap.get(q);
            double mean = rs.mean();
            double actual = feature.getDouble();
            SquaredError se = errors.get(q);
            if (se == null) {
                se = new SquaredError();
                errors.put(q, se);
            }
            se.put(actual, mean);
        }

        for (Feature q : errors.keySet()) {
            SquaredError se = errors.get(q);
            double nrmse = se.RMSE() / (allStats.max() - allStats.min());
            double cvrmse = se.RMSE() / allStats.mean();
            System.out.println(q + "\t" + se.RMSE() + "\t" + nrmse
                    + "\t" + cvrmse);
        }

        System.out.println(allStats);
    }

    public static void main(String[] args)
    throws Exception {
        String targetFeature = "temperature_surface";
        List<Feature> features = new ArrayList<>();
        for (String fileName : args) {
            System.err.println("Reading: " + fileName);
            List<Metadata> meta = ReadMetadata.readMetaBlob(new File(fileName));
            for (Metadata m : meta) {
                Feature f = m.getAttribute(targetFeature);
                if (f == null) {
                    System.err.println("Feature not found");
                    continue;
                } else {
                    System.out.println(f.getString());
                    features.add(f);
                }
            }
        }
        System.exit(0);

        TickEvaluator te = new TickEvaluator(
                TestConfiguration.quantizers.get(targetFeature));
        te.train(features);
        te.evaluate(features);
    }
}
