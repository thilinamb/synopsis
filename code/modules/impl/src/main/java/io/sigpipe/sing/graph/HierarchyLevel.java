package io.sigpipe.sing.graph;

import io.sigpipe.sing.dataset.feature.FeatureType;

/**
 * Tracks information about each level in the graph hierarchy.
 */
public class HierarchyLevel {

    public HierarchyLevel(int order, FeatureType type) {
        this.order = order;
        this.type = type;
    }

    public int order;
    public FeatureType type;

}
