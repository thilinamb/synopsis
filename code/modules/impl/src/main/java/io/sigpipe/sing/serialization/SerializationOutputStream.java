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

package io.sigpipe.sing.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class SerializationOutputStream extends DataOutputStream {

    private int compressionLevel = Deflater.DEFAULT_COMPRESSION;

    public SerializationOutputStream(OutputStream out) {
        super(out);
    }

    /**
     * Writes a String to the output stream.  The field is prefixed with the
     * String length.  null Strings are not allowed here; use an empty String
     * instead.
     *
     * @param field The String to write to the output stream
     */
    public void writeString(String field)
    throws IOException {
        writeUTF(field);
    }

    /**
     * Writes a byte array (byte field) to the output stream.  When serialized,
     * the array will be prefixed with its length.
     *
     * @param field The byte array to write to the output stream
     */
    public void writeField(byte[] field)
    throws IOException {
        writeInt(field.length);
        write(field);
    }

    /**
     * Writes a field that can be compressed.  This method is similar to
     * writeField(), but has an additional boolean flag that tells the
     * serialization framework whether or not the data is compressed.  This is
     * helpful in situations where compression might be toggled on or off by the
     * user at run time or during configuration.
     *
     * @param field The byte array to write to the stream
     * @param compress Whether the byte field should be compressed or not.
     */
    public void writeCompressableField(byte[] field, boolean compress)
    throws IOException {

        writeBoolean(compress);

        if (compress) {
            ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream();
            GZIPOutputStream gOut = new GZIPOutputStream(compressedBytes) {
                {
                    /* 1-9, where 9 = best compression */
                    def.setLevel(compressionLevel);
                }
            };

            gOut.write(field);
            gOut.close();

            byte[] compressedArray = compressedBytes.toByteArray();
            writeField(compressedArray);
        } else {
            writeField(field);
        }
    }

    public void writeSerializableCollection(
            Collection<? extends ByteSerializable> object)
    throws IOException {
        writeInt(object.size());
        for (ByteSerializable item : object) {
            writeSerializable(item);
        }
    }

    public void writeStringCollection(Collection<String> collection)
    throws IOException {
        writeInt(collection.size());
        for (String str : collection) {
            writeString(str);
        }
    }

    public void writeSimpleMap(SimpleMap<?, ? extends ByteSerializable> map)
    throws IOException {
        writeInt(map.size());
        for (ByteSerializable item : map.values()) {
            writeSerializable(item);
        }
    }

    /**
     * Writes a {@link ByteSerializable} object to this output stream.  This
     * method is equivalent to simply calling object.serialize() directly, but
     * fits into the normal serialization "flow" a bit better because it matches
     * the other methods in this class.
     *
     * @param object ByteSerializable object to serialize.
     */
    public void writeSerializable(ByteSerializable object)
    throws IOException {
        object.serialize(this);
    }

    /**
     * Sets the compression level (1-9) when writing serializable fields using
     * gzip.
     *
     * @param compressionLevel compression level this output stream will use
     * when writing compressible fields.
     */
    public void setCompressionLevel(int compressionLevel) {
        if (compressionLevel < 1 || compressionLevel > 9) {
            throw new IllegalArgumentException("gzip compression level must be "
                    + "between 1 and 9.");
        }
        this.compressionLevel = compressionLevel;
    }
}
