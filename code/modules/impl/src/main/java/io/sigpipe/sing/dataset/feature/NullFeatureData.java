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

import java.io.IOException;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * Represents null Feature data (a named feature with no related information).
 *
 * @author malensek
 */
public class NullFeatureData
extends FeatureData<Integer> implements ByteSerializable {

    public NullFeatureData() {
        this.type = FeatureType.NULL;
    }

    @Override
    public int toInt() {
        return 0;
    }

    @Override
    public long toLong() {
        return 0l;
    }

    @Override
    public float toFloat() {
        return 0f;
    }

    @Override
    public double toDouble() {
        return 0d;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public byte[] toBytes() {
        return null;
    }

    @Override
    public int compareTo(FeatureData<?> featureData) {
        if (featureData.getType() == FeatureType.NULL) {
            return 0;
        } else {
            return Integer.MIN_VALUE;
        }
    }

    @Deserialize
    public NullFeatureData(SerializationInputStream in)
    throws IOException {
        this.type = FeatureType.NULL;
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        /* Do nothing */
    }
}
