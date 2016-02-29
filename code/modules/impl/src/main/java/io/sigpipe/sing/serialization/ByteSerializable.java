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

import java.io.IOException;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes an interface for classes that can be serialized to portable
 * byte form.
 */
public interface ByteSerializable {

    /**
     * Annotates constructors used for creating new object instances from a
     * {@link SerializationInputStream}.
     *
     * This annotation is intended to increase code readability and also ensure
     * that a constructor with a SerializationInputStream is intended for
     * deserialization purposes.
     */
    @Target(ElementType.CONSTRUCTOR)
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface Deserialize { }

    /**
     * Serializes this object to binary form by passing it through a
     * serialization stream.
     *
     * @param out stream to serialize to.
     */
    public void serialize(SerializationOutputStream out) throws IOException;
}
