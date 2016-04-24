package io.sigpipe.sing.graph;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import io.sigpipe.sing.dataset.Pair;
import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.stat.RunningStatisticsND;
import io.sigpipe.sing.util.TestConfiguration;

public class Sketch {

    private GraphMetrics metrics = new GraphMetrics();
    public GeoTrie geoTrie = new GeoTrie();

    private static final Logger logger = Logger.getLogger("io.sigpipe.sing");

    /** The root vertex. */
    private Vertex root = new Vertex();

    /** Describes each level in the Feature hierarchy. */
    private Map<String, HierarchyLevel> levels = new HashMap<>();

    /**
     * We maintain a separate Queue with Feature names inserted in
     * hierarchical order.  While levels.keySet() contains the same information,
     * there is no contractual obligation for HashMap to return the keyset in
     * the original insertion order (although in practice, it probably does).
     */
    private Queue<String> features = new LinkedList<>();

    /**
     * Tracks information about each level in the graph hierarchy.
     */
    private class HierarchyLevel {

        public HierarchyLevel(int order, FeatureType type) {
            this.order = order;
            this.type = type;
        }

        public int order;
        public FeatureType type;

    }

    public Sketch() {

    }

    /**
     * Creates a HierarchicalGraph with a set Feature hierarchy.  Features are
     * entered into the hierarchy in the order they are received.
     *
     * @param hierarchy Graph hierarchy represented as a
     * {@link FeatureHierarchy}.
     */
    public Sketch(FeatureHierarchy hierarchy) {
        for (Pair<String, FeatureType> feature : hierarchy) {
            getOrder(feature.a, feature.b);
        }
    }

    /**
     * When a path does not contain a particular Feature, we use a null feature
     * (FeatureType.NULL) to act as a "wildcard" in the graph so that the path
     * stays linked together. The side effect of this is that 'less than'
     * comparisons may return wildcards, which are removed with this method.
     *
     * @param map The map to remove the first NULL element from. If the map has
     * no elements or the first element is not a NULL FeatureType, then no
     * modifications are made to the map.
     */
    private void removeWildcard(NavigableMap<Feature, Vertex> map) {
        if (map.size() <= 0) {
            return;
        }

        Feature first = map.firstKey();
        if (first.getType() == FeatureType.NULL) {
            map.remove(first);
        }
    }

    /**
     * Adds a new {@link Path} to the Hierarchical Graph.
     */
    public void addPath(Path path)
    throws FeatureTypeMismatchException, GraphException {
        if (path.size() == 0) {
            throw new GraphException("Attempted to add empty path!");
        }

        //TODO this mess really needs to be fixed up.
        Iterator<Vertex> it = path.iterator();
        while (it.hasNext()) {
            Vertex v = it.next();
            Quantizer q = TestConfiguration.quantizers.get(
                    v.getLabel().getName());
            if (q == null) {
                if (v.getLabel().getName().equals("location")) {
                    continue;
                }

                it.remove();
                continue;
            }
            boolean ok = false;
            for (String featureName : TestConfiguration.FEATURE_NAMES) {
                if (featureName.equals(v.getLabel().getName()) == true) {
                    ok = true;
                    break;
                }
            }
            if (ok == false) {
                it.remove();
                continue;
            }

            Feature quantizedFeature = q.quantize(v.getLabel());
            v.setLabel(
                    new Feature(
                        v.getLabel().getName().intern(),
                        quantizedFeature));
        }

        checkFeatureTypes(path);
        addNullFeatures(path);
        reorientPath(path);
        optimizePath(path);

        double[] values = new double[path.size() - 1];
        for (int i = 0; i < path.size() - 1; ++i) {
            values[i] = path.get(i).getLabel().getDouble();
        }
        RunningStatisticsND rsnd = new RunningStatisticsND(values);
        DataContainer container = new DataContainer(rsnd);

        /* Place the path payload (traversal result) at the end of this path. */
        path.get(path.size() - 1).setData(container);

        GraphMetrics oldMetrics = null;
        try {
            oldMetrics = (GraphMetrics) this.metrics.clone();
        } catch (Exception e) { }
        root.addPath(path.iterator(), this.metrics);

        if (oldMetrics.equals(this.metrics) == false) {
            long a = this.metrics.getVertexCount() - oldMetrics.getVertexCount();
            long b = this.metrics.getLeafCount() - oldMetrics.getLeafCount();
            geoTrie.addHash(
                    path.get(path.size() - 1).getLabel().getString(),
                    new CountContainer(a, b));
        }
    }

    /**
     * This method ensures that the Features in the path being added have the
     * same FeatureTypes as the current hierarchy.  This ensures that different
     * FeatureTypes (such as an int and a double) get placed on the same level
     * in the hierarchy.
     *
     * @param path the Path to check for invalid FeatureTypes.
     *
     * @throws FeatureTypeMismatchException if an invalid type is found
     */
    private void checkFeatureTypes(Path path)
    throws FeatureTypeMismatchException {
        for (Feature feature : path.getLabels()) {

            /* If this feature is NULL, then it's effectively a wildcard. */
            if (feature.getType() == FeatureType.NULL) {
                continue;
            }

            HierarchyLevel level = levels.get(feature.getName());
            if (level != null) {
                if (level.type != feature.getType()) {
                    throw new FeatureTypeMismatchException(
                            "Feature insertion at graph level " + level.order
                            + " is not possible due to a FeatureType mismatch. "
                            + "Expected: " + level.type + ", "
                            + "found: " + feature.getType() + "; "
                            + "Feature: <" + feature + ">");
                }
            }
        }
    }

    /**
     * For missing feature values, add a null feature to a path.  This maintains
     * the graph structure for sparse schemas or cases where a feature reading
     * is not available.
     */
    private void addNullFeatures(Path path) {
        Set<String> unknownFeatures = new HashSet<>(levels.keySet());
        for (Feature feature : path.getLabels()) {
            unknownFeatures.remove(feature.getName());
        }

        /* Create null features for missing values */
        for (String featureName : unknownFeatures) {
            Vertex v = new Vertex();
            v.setLabel(new Feature(featureName));
            path.add(v);
        }
    }

    /**
     * Reorients a nonhierarchical path in place to match the current graph
     * hierarchy.
     */
    private void reorientPath(Path path) {
        if (path.size() == 1) {
            /* This doesn't need to be sorted... */
            getOrder(path.get(0).getLabel());
            return;
        }

        path.sort(new Comparator<Vertex>() {
            public int compare(Vertex a, Vertex b) {
                int o2 = getOrder(b.getLabel());
                int o1 = getOrder(a.getLabel());
                return o1 - o2;
            }
        });
    }

    /**
     * Perform optimizations on a path to reduce the number of vertices inserted
     * into the graph.
     */
    private void optimizePath(Path path) {
        /* Remove all trailing null features.  During a traversal, trailing null
         * features are unnecessary to traverse. */
        for (int i = path.size() - 1; i >= 0; --i) {
            if (path.get(i).getLabel().getType() == FeatureType.NULL) {
                path.remove(i);
            } else {
                break;
            }
        }
    }

    /**
     * Removes all null Features from a path.  This includes any Features that
     * are the standard Java null, or Features with a NULL FeatureType.
     *
     * @param path Path to remove null Features from.
     */
    private void removeNullFeatures(Path path) {
        Iterator<Vertex> it = path.iterator();
        while (it.hasNext()) {
            Feature f = it.next().getLabel();
            if (f == null || f.getType() == FeatureType.NULL) {
                it.remove();
            }
        }
    }

    /**
     * Determines the numeric order of a Feature based on the current
     * orientation of the graph.  For example, humidity features may come first,
     * followed by temperature, etc.  If the feature in question has not yet
     * been added to the graph, then it is connected to the current leaf nodes,
     * effectively placing it at the bottom of the hierarchy, and its order
     * number is set to the current number of feature types in the graph.
     *
     * @return int representing the list ordering of the Feature
     */
    private int getOrder(String name, FeatureType type) {
        int order;
        HierarchyLevel level = levels.get(name);
        if (level != null) {
            order = level.order;
        } else {
            order = addNewFeature(name, type);
        }

        return order;
    }

    private int getOrder(Feature feature) {
        return getOrder(feature.getName(), feature.getType());
    }

    /**
     * Update the hierarchy levels and known Feature list with a new Feature.
     */
    private int addNewFeature(String name, FeatureType type) {
        logger.info("New feature: " + name + ", type: " + type);
        Integer order = levels.keySet().size();
        levels.put(name, new HierarchyLevel(order, type));
        features.offer(name);

        return order;
    }

    /**
     * Retrieves the ordering of Feature names in this graph hierarchy.
     */
    public FeatureHierarchy getFeatureHierarchy() {
        FeatureHierarchy hierarchy = new FeatureHierarchy();
        for (String feature : features) {
            try {
                hierarchy.addFeature(feature, levels.get(feature).type);
            } catch (GraphException e) {
                /* If a GraphException is thrown here, something is seriously
                 * wrong. */
                logger.severe("NULL FeatureType found in graph hierarchy!");
            }
        }
        return hierarchy;
    }

    public Vertex getRoot() {
        return root;
    }

    public GraphMetrics getMetrics() {
        return this.metrics;
    }

    public void merge(Vertex vertex, SerializationInputStream in)
    throws IOException, SerializationException {
        Feature label = new Feature(in);
        boolean hasData = in.readBoolean();
        DataContainer data = null;
        if (hasData) {
            data = new DataContainer(in);
        }

        Vertex connection = vertex.connect(
                new Vertex(label, data), true, this.metrics);

        int numNeighbors = in.readInt();
        for (int i = 0; i < numNeighbors; ++i) {
            merge(connection, in);
        }
    }

    @Override
    public String toString() {
        return root.toString();
    }
}
