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

package io.sigpipe.sing.dataset;

import java.io.IOException;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

/**
 * The basic unit of storage in Galileo.  A block could be a file, collection of
 * metadata, etc.
 *
 * @author malensek
 */
public class Block implements ByteSerializable {

    private Metadata metadata;
    private byte[] data;

    public Block(byte[] data) {
        this.metadata = new Metadata();
        this.data = data;
    }

    public Block(Metadata metadata, byte[] data) {
        this.metadata = metadata;
        this.data = data;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public byte[] getData() {
        return data;
    }

    @Deserialize
    public Block(SerializationInputStream in)
    throws IOException, SerializationException {
        this.metadata = new Metadata(in);
        data = in.readField();
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeSerializable(metadata);
        out.writeField(data);
    }
}
