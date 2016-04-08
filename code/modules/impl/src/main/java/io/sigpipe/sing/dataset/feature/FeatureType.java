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

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the various {@link Feature} types supposed by Galileo's native
 * data format.
 *
 * @author malensek
 */
public enum FeatureType {
    NULL(0),
    INT(1),
    LONG(2),
    FLOAT(3),
    DOUBLE(4),
    STRING(5),
    BINARY(6);

    private final int type;

    private FeatureType(int type) {
        this.type = type;
    }

    public int toInt() {
        return type;
    }

    static Map<Integer, FeatureType> typeMap = new HashMap<>();

    static {
        for (FeatureType t : FeatureType.values()) {
            typeMap.put(t.toInt(), t);
        }
    }

    /**
     * Determine the FeatureType associated with the given integer.
     *
     * @param i Integer representing a FeatureType element.
     *
     * @return the associated FeatureType for the integer, or null if no
     * FeatureType exists.
     */
    public static FeatureType fromInt(int i) {
        FeatureType t = typeMap.get(i);
        if (t == null) {
            return FeatureType.NULL;
        }

        return t;
    }

    static Map<Class<?>, FeatureType> primitiveMap = new HashMap<>();

    static {
        primitiveMap.put(Integer.class, INT);
        primitiveMap.put(Long.class, LONG);
        primitiveMap.put(Float.class, FLOAT);
        primitiveMap.put(Double.class, DOUBLE);
        primitiveMap.put(String.class, STRING);
        primitiveMap.put(Byte[].class, BINARY);
        primitiveMap.put(ByteArray.class, BINARY);
    }

    /**
     * Basic Feature types based on Java classes can be ascertained by
     * inspecting their generic parameters.  Other FeatureTypes, such as
     * intervals, can't be discovered by this method.
     *
     * @param type The basic Java type to inspect
     *
     * @return Corresponding FeatureType for the Java type, or null if no
     * FeatureType exists.
     */
    public static <T> FeatureType fromPrimitiveType(T type) {
        return primitiveMap.get(type.getClass());
    }

    /**
     * Provides a mapping between FeatureType elements and their corresponding
     * FeatureData implementations.
     */
    public Class<? extends FeatureData<?>> toClass() {
        switch (this) {
            case NULL: return NullFeatureData.class;
            case INT: return IntegerFeatureData.class;
            case LONG: return LongFeatureData.class;
            case FLOAT: return FloatFeatureData.class;
            case DOUBLE: return DoubleFeatureData.class;
            case STRING: return StringFeatureData.class;
            case BINARY: return BinaryFeatureData.class;
            default: return null;
        }
    }
}
