package io.sigpipe.sing.query;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.graph.GraphMetrics;
import io.sigpipe.sing.graph.Vertex;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class RelationalQuery extends Query {

    protected Set<Vertex> pruned;
    protected GraphMetrics metrics;

    public RelationalQuery() {

    }

    public RelationalQuery(GraphMetrics metrics) {
        this.metrics = metrics;
    }

    public int numPruned() {
        return this.pruned.size();
    }

    @Override
    public void execute(Vertex root)
    throws IOException, QueryException {
        if (this.metrics != null) {
            /* To make sure we don't spend time resizing the pruned HashSet, set
             * it to the number of vertices in the graph divided by the default
             * load factor. */
            this.pruned = new HashSet<Vertex>(
                    (int) (metrics.getVertexCount() / 0.75));
        } else {
            this.pruned = new HashSet<>();
        }

        prune(root, 0);
    }

    public void serializeResults(Vertex vertex, SerializationOutputStream out)
    throws IOException {
        if (pruned.contains(vertex)) {
            return;
        }

        vertex.getLabel().serialize(out);
        out.writeBoolean(vertex.hasData());
        if (vertex.hasData() == true) {
            vertex.getData().serialize(out);
        }

        /* How many neighbors are still valid after the pruning process? */
        int validNeighbors = 0;
        for (Vertex v : vertex.getAllNeighbors()) {
            if (pruned.contains(v) == false) {
                validNeighbors++;
            }
        }
        out.writeInt(validNeighbors);

        for (Vertex v : vertex.getAllNeighbors()) {
            if (pruned.contains(v) == false) {
                serializeResults(v, out);
            }
        }
    }

    private boolean prune(Vertex vertex, int expressionsEvaluated)
    throws QueryException {
        if (expressionsEvaluated == this.expressions.size()) {
            /* There are no further expressions to evaluate. Therefore, we must
             * assume all children from this point are relevant to the query. */
            return true;
        }

        boolean foundSubMatch = false;
        String childFeature = vertex.getFirstNeighbor().getLabel().getName();
        List<Expression> expList = this.expressions.get(childFeature);
        if (expList != null) {
            Set<Vertex> matches = evaluate(vertex, expList);
            if (matches.size() == 0) {
                pruned.add(vertex);
                return false;
            }

            for (Vertex match : matches) {
                if (match == null) {
                    continue;
                }

                if (match.getLabel().getType() == FeatureType.NULL) {
                    continue;
                }

                if (prune(match, expressionsEvaluated + 1) == true) {
                    foundSubMatch = true;
                }
            }

            Set<Vertex> nonMatches = new HashSet<>(vertex.getAllNeighbors());
            nonMatches.removeAll(matches);
            for (Vertex nonMatch : nonMatches) {
                pruned.add(nonMatch);
            }
        } else {
            /* No expression operates on this vertex. Consider all children. */
            for (Vertex neighbor : vertex.getAllNeighbors()) {
                if (prune(neighbor, expressionsEvaluated) == true) {
                    foundSubMatch = true;
                }
            }
        }

        if (foundSubMatch == false) {
            pruned.add(vertex);
        }

        return foundSubMatch;
    }
}
