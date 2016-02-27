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

import java.nio.ByteBuffer;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * Stores binary feature information.
 *
 * @author malensek
 */
public class BinaryFeatureData
extends FeatureData<ByteArray> implements ByteSerializable {

    public BinaryFeatureData(ByteArray data) {
        super(data);
    }

    public BinaryFeatureData(byte[] data) {
        super(new ByteArray(data));
    }

    @Override
    public int toInt() {
        ByteBuffer buffer = ByteBuffer.wrap(this.data.getBytes());
        return buffer.getInt();
    }

    @Override
    public long toLong() {
        ByteBuffer buffer = ByteBuffer.wrap(this.data.getBytes());
        return buffer.getLong();
    }

    @Override
    public float toFloat() {
        ByteBuffer buffer = ByteBuffer.wrap(this.data.getBytes());
        return buffer.getFloat();
    }

    @Override
    public double toDouble() {
        ByteBuffer buffer = ByteBuffer.wrap(this.data.getBytes());
        return buffer.getDouble();
    }

    @Override
    public String toString() {
        return new String(this.data.getBytes());
    }

    @Override
    public byte[] toBytes() {
        return this.data.getBytes();
    }

    @Deserialize
    public BinaryFeatureData(SerializationInputStream in)
    throws IOException {
        super(new ByteArray(in.readField()));
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeField(data.getBytes());
    }
}
