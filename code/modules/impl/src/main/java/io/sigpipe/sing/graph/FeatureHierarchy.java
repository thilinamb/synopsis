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

import io.sigpipe.sing.dataset.Pair;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Describes how a graph hierarchy is ordered; includes the name and type of
 * the features for each level in the graph.
 *
 * @author malensek
 */
public class FeatureHierarchy implements Iterable<Pair<String, FeatureType>> {

    List<Pair<String, FeatureType>> order = new ArrayList<>();

    private void checkType(FeatureType type)
    throws GraphException {
        if (type == FeatureType.NULL) {
            throw new GraphException("NULL Features are not allowed in a "
                    + "FeatureHierarchy.");
        }
    }

    public void addFeature(int level, Feature feature)
    throws GraphException {
        checkType(feature.getType());
        order.add(level, new Pair<>(feature.getName(), feature.getType()));
    }

    public void addFeature(Feature feature)
    throws GraphException {
        checkType(feature.getType());
        order.add(new Pair<>(feature.getName(), feature.getType()));
    }

    public void addFeature(String name, FeatureType type)
    throws GraphException {
        checkType(type);
        order.add(new Pair<>(name, type));
    }

    public List<Pair<String, FeatureType>> getHierarchy() {
        return order;
    }

    public int size() {
        return order.size();
    }

    @Override
    public Iterator<Pair<String, FeatureType>> iterator() {
        return order.iterator();
    }
}
