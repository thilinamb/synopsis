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

package io.sigpipe.sing.dataset.feature;

import io.sigpipe.sing.dataset.Pair;

/**
 * Abstract implementation of arbitrary pairs of numeric Feature data
 * represented as an interval.
 *
 * @author malensek
 */
abstract class IntervalFeatureData<T extends Number & Comparable<T>>
extends NumericFeatureData<T> {

    protected T data2;

    @Override
    public Pair<Integer, Integer> toIntInterval() {
        return new Pair<Integer, Integer>(data.intValue(), data2.intValue());
    }

    @Override
    public Pair<Long, Long> toLongInterval() {
        return new Pair<Long, Long>(data.longValue(), data2.longValue());
    }

    @Override
    public Pair<Float, Float> toFloatInterval() {
        return new Pair<Float, Float>(data.floatValue(), data2.floatValue());
    }

    @Override
    public Pair<Double, Double> toDoubleInterval() {
        return new Pair<Double, Double>(data.doubleValue(),
                data2.doubleValue());
    }

    @Override
    public byte[] toBytes() {
        return new byte[] { data.byteValue(), data2.byteValue() };
    }

    @Override
    public String toString() {
        return "[" + data.toString() + " .. " + data2.toString() + "]";
    }
}
