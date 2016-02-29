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

import io.sigpipe.sing.serialization.ByteSerializable;

/**
 * Wrapper for features based on java.lang.Number.
 *
 * @author malensek
 */
abstract class NumericFeatureData<T extends Number & Comparable<T>>
extends FeatureData<T> implements ByteSerializable {

    public NumericFeatureData() { }

    public NumericFeatureData(T data) {
        super(data);
    }

    @Override
    public int toInt() {
        return this.data.intValue();
    }

    @Override
    public long toLong() {
        return this.data.longValue();
    }

    @Override
    public float toFloat() {
        return this.data.floatValue();
    }

    @Override
    public double toDouble() {
        return this.data.doubleValue();
    }

    @Override
    public byte[] toBytes() {
        return new byte[] { this.data.byteValue() };
    }
}
