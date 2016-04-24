package io.sigpipe.sing.query;

import java.io.IOException;
import java.util.Iterator;

import io.sigpipe.sing.graph.GraphMetrics;
import io.sigpipe.sing.graph.Vertex;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class PartitionQuery extends RelationalQuery {

    public PartitionQuery(GraphMetrics metrics) {
        super(metrics);
    }

    @Override
    public void serializeResults(
            Vertex vertex, SerializationOutputStream out)
    throws IOException {
        serializeAndDeleteResults(vertex, out);
    }

    /**
     * @return true if the Vertex this method was called on can be deleted;
     * Vertices are deletable if the query matched it, AND all of its children.
     */
    private boolean serializeAndDeleteResults(
            Vertex vertex, SerializationOutputStream out)
    throws IOException {
        if (pruned.contains(vertex)) {
            /* A pruned (non-matching) vertex cannot be deleted */
            return false;
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

        boolean deletable = true;
        if (validNeighbors != vertex.numNeighbors()) {
            /* If some of this vertex's children didn't match the query, it
             * cannot be deleted */
            deletable = false;
        }

        Iterator<Vertex> it = vertex.getAllNeighbors().iterator();
        while (it.hasNext()) {
            Vertex v = it.next();
            if (pruned.contains(v) == false) {
                if (serializeAndDeleteResults(v, out) == true) {
                    this.metrics.removeVertex();
                    if (v.hasData()) {
                        this.metrics.removeLeaf();
                    }
                    it.remove();
                } else {
                    deletable = false;
                }
            }
        }

        return deletable;
    }
}

