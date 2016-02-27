package io.sigpipe.sing.stat;

import java.util.HashMap;
import java.util.Map;

import io.sigpipe.sing.dataset.feature.Feature;

public class FeatureSurvey {

    private Map<String, RunningStatistics> stats = new HashMap<>();

    public FeatureSurvey() {

    }

    public void add(Feature feature) {
        RunningStatistics rs = stats.get(feature.getName());
        if (rs == null) {
            rs = new RunningStatistics();
            stats.put(feature.getName(), rs);
        }
        rs.put(feature.getDouble());
    }

    public void printAll() {
        for (String featureName : stats.keySet()) {
            System.out.println("--- " + featureName + " ---");
            RunningStatistics rs = stats.get(featureName);
            System.out.println(rs);
            System.out.println();
        }
    }
}
