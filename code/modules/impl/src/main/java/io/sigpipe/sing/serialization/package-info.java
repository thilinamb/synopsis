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

/**
 * Handles serializing/deserializing data to/from the native elssa binary
 * format.  This package is similar to the Java Externalizable interface, but
 * has some convenience features to help ease manual serialization.
 * <p>
 * Classes that can be serialized using this framework should implement the
 * {@link io.elssa.serialization.ByteSerializable} interface, which mandates a
 * serialize() method that converts the implementing class to a binary
 * representation.  Rather than enforcing an empty constructor and a
 * deserialize() method, implementing classes are expected to have a constructor
 * that takes a single SerializationInputStream and uses it to deserialize from
 * the binary format and initialize the object.
 */
package io.sigpipe.sing.serialization;
