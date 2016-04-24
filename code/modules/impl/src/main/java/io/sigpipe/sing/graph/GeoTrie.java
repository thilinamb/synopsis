/*
Copyright (c) 2016, Colorado State University
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
import java.util.Iterator;
import java.util.List;

import io.sigpipe.sing.dataset.feature.Feature;

public class GeoTrie {

    private Vertex root = new Vertex();

    public GeoTrie() {

    }

    public void addHash(String geohash, CountContainer payload) {
        List<Vertex> path = hashToPath(geohash.toLowerCase());
        path.get(path.size() - 1).setData(payload);
        root.addPath(path.iterator());
    }

    public CountContainer query(String geohash) {
        List<Vertex> path = hashToPath(geohash.toLowerCase());
        CountContainer cc = new CountContainer();
        query(root, path.iterator(), cc);
        return cc;
    }

    private void query(
            Vertex vertex, Iterator<Vertex> path, DataContainer container) {
        if (path.hasNext()) {
            Vertex queryVertex = path.next();
            Vertex neighbor = vertex.getNeighbor(queryVertex.getLabel());
            if (neighbor == null) {
                /* Specified hash character wasn't found, there are no matches
                 * for this query. */
                return;
            } else {
                query(neighbor, path, container);
            }
        } else {
            for (Vertex v : vertex.getAllNeighbors()) {
                query(v, path, container);
            }
        }

        if (vertex.hasData()) {
            container.merge(vertex.getData());
        }
    }

    private Vertex findVertex(String geohash) {
        return findVertex(this.root, hashToPath(geohash).iterator());
    }

    private Vertex findVertex(Vertex start, Iterator<Vertex> path) {
        if (path.hasNext()) {
            Vertex step = path.next();
            Vertex neighbor = start.getNeighbor(step.getLabel());
            if (neighbor == null) {
                /* Part of the path is broken; return null */
                return null;
            } else {
                return findVertex(neighbor, path);
            }
        } else {
            return start;
        }
    }

    public void remove(String geohash) {
        if (geohash.equals("")) {
            /* Removing a blank string selects all vertices for removal */
            root.disconnectAll();
            return;
        }

        remove(this.root, hashToPath(geohash).iterator());
    }

    private void remove(Vertex vertex, Iterator<Vertex> path) {
        if (path.hasNext()) {
            Vertex search = path.next();
            Vertex neighbor = vertex.getNeighbor(search.getLabel());
            if (neighbor == null) {
                /* Part of the path is broken; return null */
                return;
            } else {
                if (path.hasNext() == false) {
                    /* The next vertex is the last one in the search path */
                    System.out.println("removing: " + search.getLabel());
                    vertex.disconnect(search.getLabel());
                    System.out.println(vertex.numNeighbors());
                } else {
                    /* Keep going */
                    remove(neighbor, path);
                }
            }
        }
    }

    private List<Vertex> hashToPath(String geohash) {
        List<Vertex> path = new ArrayList<>(geohash.length());
        for (char c : geohash.toCharArray()) {
            path.add(
                    new Vertex(
                        new Feature("geohash", String.valueOf(c))));
        }
        return path;
    }
}
