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
 * Encapsulates feature values that are represented by long integers.
 *
 * @author malensek
 */
public class LongFeatureData
extends NumericFeatureData<Long> implements ByteSerializable {

    public LongFeatureData(long data) {
        super(data);
    }

    @Override
    public Feature add(Feature f) {
        return new Feature(this.data + f.getLong());
    }

    @Override
    public Feature subtract(Feature f) {
        return new Feature(this.data - f.getLong());
    }

    @Override
    public Feature divide(Feature f) {
        return new Feature(this.data / f.getLong());
    }

    @Override
    public Feature multiply(Feature f) {
        return new Feature(this.data * f.getLong());
    }

    @Deserialize
    public LongFeatureData(SerializationInputStream in)
    throws IOException {
        super(in.readLong());
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeLong(this.data);
    }
}
