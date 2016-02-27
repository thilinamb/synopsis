/*
Copyright (c) 2014, Colorado State University
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

import io.sigpipe.sing.dataset.feature.Feature;

import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Represents the buckets described by "Tick Marks" in the system.  For example,
 * a TickHash with values 0, 10, 20, and 30 would output "30" for an input of
 * 33, "0" for an input of 5, etc.  It allows incoming samples to be placed into
 * coarser-grained buckets.  Additionally, the first and last ticks stretch off
 * to infinity; Feature-specific constraints on the tick marks should be managed
 * by classes using TickHash.
 *
 * @author malensek
 */
public class TickHash {

    private NavigableSet<Feature> tickSet = new TreeSet<>();
    private Feature low;
    private Feature high;

    public TickHash(Feature... features) {
        for (Feature f : features) {
            addTick(f);
        }
    }

    public void addTick(Feature feature) {
        if (low == null || feature.less(low)) {
            low = feature;
        }

        if (high == null || feature.greater(high)) {
            high = feature;
        }

        tickSet.add(feature);
    }

    public NavigableSet<Feature> getTicks() {
        return tickSet;
    }

    public Feature getBucket(Feature feature) {
        if (feature.greater(high)) {
            return high;
        }

        if (feature.less(low)) {
            return low;
        }

        return tickSet.floor(feature);
    }

    @Override
    public String toString() {
        String str = "";
        for (Feature f : tickSet) {
            str += f.dataToString() + " - ";
        }
        return str.substring(0, str.length() - 3);
    }
}
