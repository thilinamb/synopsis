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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * Provides a lightweight generic implementation of a graph vertex backed by a
 * TreeMap for extensibility.  This provides the basis of the hybrid
 * trees/graphs used in the system. Vertices store labels that extend the
 * Comparable interface to ensure they can be ordered properly in the TreeMap.
 *
 * @author malensek
 */
public class Vertex implements ByteSerializable {

    protected Feature label;
    protected DataContainer data;
    protected TreeMap<Feature, Vertex> edges = new TreeMap<>();

    public Vertex() {
        label = new Feature();
    }

    public Vertex(Feature label) {
        this.label = label;
    }

    public Vertex(Feature label, DataContainer data) {
        this.label = label;
        this.data = data;
    }

    public Vertex(Vertex v) {
        this.label = v.label;
        this.data = v.data;
    }

    /**
     * Determines whether two vertices are connected.
     *
     * @param label the label of the vertex to search for
     * @return true if the Vertex label is found on a connecting edge.
     */
    public boolean connectedTo(Feature label) {
        return edges.containsKey(label);
    }

    /**
     * Determines whether two vertices are connected.
     *
     * @param v the vertex to search for
     * @return true if the Vertex is found on a connecting edge.
     */
    public boolean connectedTo(Vertex v) {
        return edges.containsValue(v);
    }

    /**
     * Retrieve a neighboring Vertex.
     *
     * @param label Neighbor's label.
     * @return Neighbor Vertex.
     */
    public Vertex getNeighbor(Feature label) {
        return edges.get(label);
    }

    public Vertex getFirstNeighbor() {
        return edges.firstEntry().getValue();
    }

    /**
     * Retrieves the {@link NavigableMap} of neighboring vertices less than the
     * specified value.
     *
     * @param label label value to compare against
     * @param inclusive whether or not to include the label's value while doing
     *     comparisons
     * @return {@link NavigableMap} of neighboring vertices
     */
    public NavigableMap<Feature, Vertex> getNeighborsLessThan(
            Feature label, boolean inclusive) {
        return edges.headMap(label, inclusive);
    }

    /**
     * Retrieves the {@link NavigableMap} of neighboring vertices greater than
     * the specified value.
     *
     * @param label label value to compare against
     * @param inclusive whether or not to include the label's value while doing
     *     comparisons
     * @return {@link NavigableMap} of neighboring vertices
     */
    public NavigableMap<Feature, Vertex> getNeighborsGreaterThan(
            Feature label, boolean inclusive) {
        return edges.tailMap(label, inclusive);
    }

    /**
     * Retrieves the {@link NavigableMap} of neighboring vertices within the
     * range specified.
     *
     * @param from the beginning of the range (inclusive)
     * @param to the end of the range (exclusive)
     * @return {@link NavigableMap} of neighboring vertices in the specified
     *     range
     */
    public NavigableMap<Feature, Vertex> getNeighborsInRange(
            Feature from, Feature to) {

        return getNeighborsInRange(from, true, to, false);
    }

    /**
     * Retrieves the {@link NavigableMap} of neighboring vertices within the
     * range specified.
     *
     * @param from the beginning of the range
     * @param fromInclusive whether to include 'from' in the range of values
     * @param to the end of the range (exclusive)
     * @param toInclusive whether to include 'to' in the range of values
     * @return {@link NavigableMap} of neighboring vertices in the specified
     *     range
     */
    public NavigableMap<Feature, Vertex> getNeighborsInRange(
            Feature from, boolean fromInclusive,
            Feature to, boolean toInclusive) {

        return edges.subMap(from, fromInclusive, to, toInclusive);
    }

    /**
     * Retrieve the labels of all neighboring vertices.
     *
     * @return Neighbor Vertex labels.
     */
    public Set<Feature> getNeighborLabels() {
        return edges.keySet();
    }

    /**
     * Traverse all edges to return all neighboring vertices.
     *
     * @return collection of all neighboring vertices.
     */
    public Collection<Vertex> getAllNeighbors() {
        return edges.values();
    }

    public int numNeighbors() {
        return edges.size();
    }

    /**
     * Connnects two vertices.  If this vertex is already connected to the
     * provided vertex label (based on value), then the already-connected vertex
     * is returned and its DataContainer will be merged with the vertex passed
     * to this method.
     *
     * @param vertex The vertex to connect to.
     * @return Connected vertex.
     */
    protected Vertex connect(Vertex v) {
        return connect(v, false, null);
    }

    /**
     * Connnects two vertices.  If this vertex is already connected to the
     * provided vertex label (based on value), then the already-connected vertex
     * is returned and its DataContainer will be merged with the vertex passed
     * to this method.
     *
     * @param vertex The vertex to connect to.
     * @param overwriteData If set to true, any DataContainers in the
     *     destination vertex will be overwritten instead of merged.
     * @return Connected vertex.
     */
    protected Vertex connect(Vertex v, boolean overwriteData) {
        return connect(v, overwriteData, null);
    }

    /**
     * Connnects two vertices.  If this vertex is already connected to the
     * provided vertex label (based on value), then the already-connected vertex
     * is returned and its DataContainer will be merged with the vertex passed
     * to this method.
     *
     * @param vertex The vertex to connect to.
     * @param overwriteData If set to true, any DataContainers in the
     *     destination vertex will be overwritten instead of merged.
     * @param metrics A {@link GraphMetrics} instance to update as the connect
     *     operation is carred out.
     * @return Connected vertex.
     */
    protected Vertex connect(
            Vertex v, boolean overwriteData, GraphMetrics metrics) {
        Feature label = v.getLabel();
        Vertex neighbor = getNeighbor(label);
        if (neighbor == null) {
            edges.put(label, v);
            if (metrics != null) {
                metrics.addVertex();
                if (v.hasData()) {
                    metrics.addLeaf();
                }
            }
            return v;
        } else {
            if (neighbor.hasData() && overwriteData == false) {
                DataContainer container = neighbor.getData();
                container.merge(v.getData());
            } else {
                neighbor.setData(v.getData());
            }
            return neighbor;
        }
    }

    public Vertex disconnect(Feature label) {
        return this.edges.remove(label);
    }

    public Vertex disconnect(Vertex vertex) {
        return this.edges.remove(vertex.label);
    }

    /**
     * Removes all the edges from this Vertex, severing any connections with
     * neighboring vertices.
     */
    public void disconnectAll() {
        edges.clear();
    }

    /**
     * Add and connect a collection of vertices in the form of a traversal path,
     * starting with this vertex.
     *
     * @param path Collection of vertices to connect
     */
    public void addPath(Iterator<Vertex> path) {
        addPath(path, null);
    }

    /**
     * Add and connect a collection of vertices in the form of a traversal path,
     * starting with this vertex.
     *
     * @param path Collection of vertices to connect
     * @param metrics A {@link GraphMetrics} instance to populate as the path is
     *     added.
     */
    public void addPath(Iterator<Vertex> path, GraphMetrics metrics) {
        if (path.hasNext()) {
            Vertex vertex = path.next();
            Vertex connection = connect(vertex, false, metrics);
            connection.addPath(path, metrics);
        }
    }

    /**
     * Retrieves the label associated with this vertex.
     */
    public Feature getLabel() {
        return label;
    }

    public void setLabel(Feature label) {
        this.label = label;
    }

    public DataContainer getData() {
        return data;
    }

    /**
     * Retrieves whether or not this vertex has an associated
     * {@link DataContainer}.
     */
    public boolean hasData() {
        return data != null;
    }

    public void setData(DataContainer container) {
        this.data = container;
    }

    /**
     * Retrieves the number of descendant vertices for this {@link Vertex}.
     *
     * @return number of descendants (children)
     */
    public long numDescendants() {
        long total = this.getAllNeighbors().size();
        for (Vertex child : this.getAllNeighbors()) {
            total += child.numDescendants();
        }

        return total;
    }

    /**
     * Retrieves the number of descendant edges for this {@link Vertex}.
     *
     * @return number of descendant edges.
     */
    public long numDescendantEdges() {
        long total = 0;
        int numNeighbors = this.getAllNeighbors().size();

        if (numNeighbors > 0) {
            total = numNeighbors + numNeighbors - 1;
        }

        for (Vertex child : this.getAllNeighbors()) {
            total += child.numDescendantEdges();
        }

        return total;
    }

    public long numLeaves() {
        long total = 0;
        if (this.numNeighbors() == 0) {
            total++;
        } else {
            for (Vertex child : this.getAllNeighbors()) {
                total += child.numLeaves();
            }
        }
        return total;
    }

    @Override
    public String toString() {
        return "V: [" + label.toString() + "] "
            + "(" + this.getAllNeighbors().size() + ")";
    }

    @Deserialize
    public Vertex(SerializationInputStream in)
    throws IOException, SerializationException {
        this(in, null);
    }

    public Vertex(SerializationInputStream in, GraphMetrics metrics)
    throws IOException, SerializationException {
        this.label = new Feature(in);
        this.data = new DataContainer(in);

        int neighbors = in.readInt();
        for (int i = 0; i < neighbors; ++i) {
            Vertex v = new Vertex(in);
            this.connect(v, false, metrics);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        this.label.serialize(out);
        if (this.hasData() == false) {
            this.data = new DataContainer();
        }
        this.data.serialize(out);
        out.writeInt(this.numNeighbors());
        for (Vertex v : this.getAllNeighbors()) {
            v.serialize(out);
        }
    }
}
