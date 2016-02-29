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

import java.util.logging.Level;
import java.util.logging.Logger;

import io.sigpipe.sing.dataset.Pair;
import io.sigpipe.sing.serialization.ByteSerializable;

/**
 * Abstract implementation of arbitrary Feature data.  Provides a number of
 * default method implementations to ease the creation of new Feature types.
 *
 * @author malensek
 */
abstract class FeatureData<T extends Comparable<T>>
implements ByteSerializable, Comparable<FeatureData<?>> {

    private static final Logger logger = Logger.getLogger("io.sigpipe.sing");

    protected T data;
    protected FeatureType type = FeatureType.NULL;

    public FeatureData() { }

    /**
     * Constructs FeatureData from a primitive or basic Java type.
     */
    public FeatureData(T data) {
        this.data = data;
        this.type = FeatureType.fromPrimitiveType(data);
    }

    /**
     * Provides the integer representation of this FeatureData.
     *
     * @return integer representation of this FeatureData.
     */
    public abstract int toInt();

    /**
     * Provides the long integer representation of this FeatureData.
     *
     * @return long representation of this FeatureData.
     */
    public abstract long toLong();

    /**
     * Provides the float representation of this FeatureData.
     *
     * @return float representation of this FeatureData.
     */
    public abstract float toFloat();

    /**
     * Provides the double precision floating-point representation of this
     * FeatureData.
     *
     * @return double representation of this FeatureData.
     */
    public abstract double toDouble();

    /**
     * Provides the integer-based interval representation of this FeatureData.
     * If the FeatureData does not contain an interval (i.e., it is just a
     * single point) then the start and end points of the interval will be the
     * same value.
     *
     * @return integer interval representation of this FeatureData.
     */
    public Pair<Integer, Integer> toIntInterval() {
        return new Pair<Integer, Integer>(this.toInt(), this.toInt());
    }

    /**
     * Provides the long integer-based interval representation of this
     * FeatureData.  If the FeatureData does not contain an interval (i.e., it
     * is just a single point) then the start and end points of the interval
     * will be the same value.
     *
     * @return long interval representation of this FeatureData.
     */
    public Pair<Long, Long> toLongInterval() {
        return new Pair<Long, Long>(this.toLong(), this.toLong());
    }

    /**
     * Provides the floating point interval representation of this FeatureData.
     * If the FeatureData does not contain an interval (i.e., it is just a
     * single point) then the start and end points of the interval will be the
     * same value.
     *
     * @return float interval representation of this FeatureData.
     */
    public Pair<Float, Float> toFloatInterval() {
        return new Pair<Float, Float>(this.toFloat(), this.toFloat());
    }

    /**
     * Provides the double-precision floating point interval representation of
     * this FeatureData.  If the FeatureData does not contain an interval (i.e.,
     * it is just a single point) then the start and end points of the interval
     * will be the same value.
     *
     * @return double interval representation of this FeatureData.
     */
    public Pair<Double, Double> toDoubleInterval() {
        return new Pair<Double, Double>(this.toDouble(), this.toDouble());
    }

    /**
     * Provides the String-based representation of this FeatureData.
     *
     * @return String representation of this FeatureData.
     */
    public String toString() {
        return data.toString();
    }

    /**
     * Retrieves the binary byte array representation of this FeatureData.
     */
    public abstract byte[] toBytes();

    /**
     * Return this FeatureData's type.
     *
     * @return the FeatureType for this FeatureData.
     */
    public FeatureType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        final int prime = 97;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        return compareTo((FeatureData<?>) obj) == 0;
    }

    @Override
    public int compareTo(FeatureData<?> featureData) {
        /* NULL vs X is handled by the NullFeatureData class.  Here we handle
         * the opposite case, X vs NULL. */
        if (featureData.getType() == FeatureType.NULL) {
            return Integer.MAX_VALUE;
        }

        try {
            /* If something goes wrong here, a ClassCastException will be
             * thrown. */
            @SuppressWarnings("unchecked")
            int compare = this.data.compareTo((T) featureData.data);
            return compare;
        } catch (ClassCastException e) {
            //TODO: should this really result in a log message? A better
            //approach may be to throw a custom error here.
            if (logger.isLoggable(Level.WARNING)) {
                FeatureData<T> a = this;
                FeatureData<?> b = featureData;

                logger.warning("Illegal comparison between Feature types! "
                        + "[" + a.getType() + ", " + a.data + "]"
                        + " cannot be compared to "
                        + "[" + b.getType() + ", " + b.data + "]");
            }
            throw e;
        }
    }
}
