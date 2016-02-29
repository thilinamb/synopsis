package io.sigpipe.sing.adapters;

import io.sigpipe.sing.graph.Vertex;
import io.sigpipe.sing.serialization.Serializer;

public class ReadVertexBundle {

    public static void main(String[] args) throws Exception {

        Vertex root = Serializer.restoreCompressed(Vertex.class, args[0]);
        System.out.println(root.numLeaves());
        System.out.println(root.numDescendants());
        System.out.println(root.numDescendantEdges());

    }
}
