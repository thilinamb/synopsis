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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import io.sigpipe.sing.dataset.feature.Feature;

/**
 * Tracks a {@link io.sigpipe.sing.query.Query} as it traverses through a graph
 * hierarchy.
 *
 * @author malensek
 */
public class HierarchicalQueryTracker<T> {

    public List<List<Path>> results = new ArrayList<>();
    private int farthestEvaluatedExpression = 0;
    private int currentLevel = 0;

    private Path rootPath;

    public HierarchicalQueryTracker(Vertex root, int numFeatures) {
        int size = numFeatures + 1;
        results = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            results.add(new ArrayList<Path>());
        }

        rootPath = new Path(root);
        List<Path> l = new ArrayList<>(1);
        l.add(rootPath);
        results.get(0).add(rootPath);
    }

    public void addResults(Path previousPath,
            Collection<Vertex> results) {

        for (Vertex vertex : results) {
            Path path = new Path(previousPath);
            path.add(vertex);

//            /* Copy over the payload */
//            if (vertex.getValues().size() > 0) {
//                path.setPayload(new HashSet<>(vertex.getValues()));
//            }

            this.results.get(getCurrentLevel()).add(path);
        }
    }

    public void nextLevel() {
        ++currentLevel;
    }

    /**
     * Retrieves the current level being processed.
     */
    public int getCurrentLevel() {
        return currentLevel;
    }

    /**
     * Retrieves the results that are currently being processed. In other words,
     * get the results from the last level in the hierarchy.
     */
    public List<Path> getCurrentResults() {
        return results.get(getCurrentLevel() - 1);
    }

    public void markEvaluated() {
        farthestEvaluatedExpression = getCurrentLevel();
    }

    public List<Path> getQueryResults() {
        List<Path> paths = new ArrayList<>();
        for (int i = farthestEvaluatedExpression; i < results.size(); ++i) {
            for (Path path : results.get(i)) {
//                if (path.hasPayload()) {
//                    paths.add(path);
//                }
            }
        }
        return paths;
    }

    @Override
    public String toString() {
        return "";
    }
}
