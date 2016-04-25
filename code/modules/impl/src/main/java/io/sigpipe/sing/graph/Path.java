/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package io.sigpipe.sing.graph;

import java.util.ArrayList;
import java.util.List;

import io.sigpipe.sing.dataset.feature.Feature;

/**
 * Represents a simple graph path.  A path contains a number of vertices and
 * edges that are connected to form a chain that traverses through a graph.
 *
 * @author malensek
 */
public class Path extends ArrayList<Vertex> {

    private static final long serialVersionUID = 8201748135149945940L;

    public Path() {

    }

    /**
     * Constructs an empty list with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity of the list
     */
    public Path(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Create a Path with a number of vertices pre-populated.
     */
    @SafeVarargs
    public Path(Vertex... vertices) {
        for (Vertex vertex : vertices) {
            this.add(vertex);
        }
    }

    /**
     * Create a Path from a number of features.
     */
    @SafeVarargs
    public Path(Feature... features) {
        for (Feature feature : features) {
            this.add(new Vertex(feature));
        }
    }

    /**
     * Creates a path by copying the vertices from an existing path.
     */
    public Path(Path p) {
        /* New Vertices must be created if this path will be used anywhere but
         * its source graph; the type hierarchy is embedded in the vertices. */
        for (Vertex v : p) {
            this.add(new Vertex(v));
        }
    }

    public void add(Feature label) {
        add(new Vertex(label));
    }

    public void add(Feature label, DataContainer value) {
        add(new Vertex(label, value));
    }

    public Vertex getTail() {
        return this.get(this.size() - 1);
    }

    /**
     * Retrieve a list of the {@link Vertex} labels in this Path.
     */
    public List<Feature> getLabels() {
        List<Feature> labels = new ArrayList<>();
        for (Vertex vertex : this) {
            labels.add(vertex.getLabel());
        }

        return labels;
    }

    @Override
    public String toString() {
        String str = "";
        for (int i = 0; i < this.size(); ++i) {
            str += this.get(i).getLabel();

            if (i < this.size() - 1) {
                str += " -> ";
            }
        }

        return str;
    }
}
