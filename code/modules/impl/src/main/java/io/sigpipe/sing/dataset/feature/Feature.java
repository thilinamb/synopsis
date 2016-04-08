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
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;
import io.sigpipe.sing.serialization.Serializer;

/**
 * Contains Feature information -- arbitrary data dimensions for storage and
 * indexing.
 *
 * @author malensek
 */
public class Feature implements Comparable<Feature>, ByteSerializable {

    protected String name;
    protected FeatureData<? extends Comparable<?>> data;

    /**
     * Constructs a nameless, null Feature (no data).
     */
    public Feature() {
        this("");
    }

    /**
     * Constructs a null Feature (no data).
     */
    public Feature(String name) {
        setName(name);
        this.data = new NullFeatureData();
    }

    /**
     * Constructs an integer Feature.
     */
    public Feature(String name, int value) {
        setName(name);
        this.data = new IntegerFeatureData(value);
    }

    /**
     * Constructs a nameless integer Feature.
     */
    public Feature(int value) {
        this("", value);
    }

    /**
     * Constructs a long Feature.
     */
    public Feature(String name, long value) {
        setName(name);
        this.data = new LongFeatureData(value);
    }

    /**
     * Constructs a nameless long Feature.
     */
    public Feature(long value) {
        this("", value);
    }

    /**
     * Constructs a floating point Feature.
     */
    public Feature(String name, float value) {
        setName(name);
        this.data = new FloatFeatureData(value);
    }

    /**
     * Constructs a nameless floating point Feature.
     */
    public Feature(float value) {
        this("", value);
    }

    /**
     * Constructs a double-precision floating point Feature.
     */
    public Feature(String name, double value) {
        setName(name);
        this.data = new DoubleFeatureData(value);
    }

    /**
     * Constructs a nameless double-precision floating point Feature.
     */
    public Feature(double value) {
        this("", value);
    }

    /**
     * Constructs a String Feature.
     */
    public Feature(String name, String value) {
        setName(name);
        this.data = new StringFeatureData(value);
    }

    /**
     * Constructs a nameless Feature that contains raw binary information
     * (byte array).
     */
    public Feature(byte[] bytes) {
        this("", bytes);
    }

    /**
     * Constructs a Feature that contains raw binary information (byte array).
     */
    public Feature(String name, byte[] bytes) {
        setName(name);
        this.data = new BinaryFeatureData(bytes);
    }

    /**
     * Creates a shallow copy of a Feature.
     */
    public Feature(Feature feature) {
        this(feature.name, feature);
    }

    /**
     * Creates a shallow copy of a Feature with the provided name.
     */
    public Feature(String name, Feature feature) {
        setName(name);
        this.data = feature.data;
    }

    /**
     * Converts a native Object to a nameless Feature.  Useful if the Object
     * type is not known in advance.
     *
     * @param o Object to convert to a Feature
     */
    public static Feature fromPrimitiveType(Object o) {
        return fromPrimitiveType("", o);
    }

    /**
     * Converts a native Object to a Feature if the Object type is not known in
     * advance.  This is done by determining if the provided object is an
     * instance of a valid Feature type, and then casting the Object to the
     * type.  In general, this method should only be used in special cases where
     * an item's type is not known already.
     *
     * @param name Name of the resulting Feature
     * @param o Object to convert to a Feature
     */
    public static Feature fromPrimitiveType(String name, Object o) {
        FeatureType type = FeatureType.fromPrimitiveType(o);
        if (type == null) {
            throw new IllegalArgumentException("Cannot construct a Feature "
                    + "from this type.");
        }

        switch (type) {
            case INT: return new Feature(name, (int) o);
            case LONG: return new Feature(name, (long) o);
            case FLOAT: return new Feature(name, (float) o);
            case DOUBLE: return new Feature(name, (double) o);
            case STRING: return new Feature(name, (String) o);
            default:
                throw new IllegalArgumentException("Could not instantiate "
                        + "FeatureData");
        }
    }

    public String getName() {
        return name;
    }

    private void setName(String name) {
        this.name = name;
    }

    public FeatureType getType() {
        return data.getType();
    }

    public boolean sameType(Feature otherFeature) {
        return this.getType() == otherFeature.getType();
    }

    public boolean isRawBytes() {
        return this.data.getType() == FeatureType.BINARY;
    }

    public int getInt() {
        return data.toInt();
    }

    public long getLong() {
        return data.toLong();
    }

    public float getFloat() {
        return data.toFloat();
    }

    public double getDouble() {
        return data.toDouble();
    }

    /**
     * If this Feature contains raw bytes, this method retrieves them as a
     * native Java byte array.
     *
     * @throws ClassCastException if the underlying FeatureData is not
     * BinaryFeatureData
     */
    public byte[] getRawBytes() {
        return ((BinaryFeatureData) this.data).toBytes();
    }

    /**
     * Retrieves the raw data container that holds this Feature data.  The raw
     * {@link FeatureData} can be used directly or casted to a subclass to
     * obtain specific functionality.
     */
    public FeatureData<?> getDataContainer() {
        return this.data;
    }

    /**
     * Returns the current Feature data as a String value.  This method is
     * different from the toString() method, which provides a String
     * representation of the Feature class rather than just its data.
     *
     * @return String representation of this Feature's data.
     */
    public String getString() {
        return data.toString();
    }

    public Feature convertTo(FeatureType type) {
        Feature f = null;
        switch (type) {
            case INT:
                f = new Feature(this.getInt());
                break;
            case LONG:
                f = new Feature(this.getLong());
                break;
            case FLOAT:
                f = new Feature(this.getFloat());
                break;
            case DOUBLE:
                f = new Feature(this.getDouble());
                break;
            case STRING:
                f = new Feature(this.getString());
                break;
            case BINARY:
                f = new Feature(this.getRawBytes());
                break;
            default:
                f = new Feature();
        }
        f.setName(this.getName());
        return f;
    }

    public Feature convertTo(Feature f) {
        return convertTo(f.getType());
    }

    /**
     * Adds two Features, and returns a new Feature containing the sum. The
     * resulting Feature will inherit this Feature's name.
     *
     * @param f The feature to add to
     * @return {@link Feature} instance containing the sum.
     */
    public Feature add(Feature f) {
        Feature result = this.data.add(f);
        result.setName(this.getName());
        return result;
    }

    /**
     * Subtracts two Features, and returns a new Feature containing the
     * difference. The resulting Feature will inherit this Feature's name.
     *
     * @param f The feature to subtact from this feature
     * @return {@link Feature} instance containing the difference.
     */
    public Feature subtract(Feature f) {
        Feature result = this.data.subtract(f);
        result.setName(this.getName());
        return result;
    }

    /**
     * Multiplies two Features, and returns a new Feature containing the
     * product.  The resulting Feature will inherit this Feature's name.
     *
     * @param f factor to multiply by
     * @return {@link Feature} instance containing the multiplied product.
     */
    public Feature multiply(Feature f) {
        Feature result = this.data.multiply(f);
        result.setName(this.getName());
        return result;
    }

    /**
     * Divides two Features, and returns a new Feature containing the quotient.
     * The resulting Feature will inherit this Feature's name.
     *
     * @param f divisor
     * @return {@link Feature} instance containing the quotient.
     */
    public Feature divide(Feature f) {
        Feature result = this.data.divide(f);
        result.setName(this.getName());
        return result;
    }

    public boolean greater(Feature f) {
        return this.compareTo(f) > 0;
    }

    public boolean greaterEqual(Feature f) {
        return this.compareTo(f) >= 0;
    }

    public boolean less(Feature f) {
        return this.compareTo(f) < 0;
    }

    public boolean lessEqual(Feature f) {
        return this.compareTo(f) <= 0;
    }

    @Override
    public int hashCode() {
        final int prime = 773;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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

        Feature other = (Feature) obj;
        if (this.data.equals(other.data)) {
            return true;
        }

        return false;
    }

    @Override
    public int compareTo(Feature otherFeature) {
        return this.data.compareTo(otherFeature.data);
    }

    public String dataToString() {
        return "" + data;
    }

    @Override
    public String toString() {
        if (name.equals("")) {
            if (data.getType() == FeatureType.NULL) {
                return "[null]";
            } else {
                return "(unnamed feature)=" + data;
            }
        }

        return name + "=" + data;
    }

    @Deserialize
    public Feature(String name, FeatureType type, SerializationInputStream in)
    throws IOException, SerializationException {
        setName(name);
        data = Serializer.deserializeFromStream(type.toClass(), in);
    }

    @Deserialize
    public Feature(SerializationInputStream in)
    throws IOException, SerializationException {
        setName(in.readString());
        FeatureType type = FeatureType.fromInt(in.readInt());
        data = Serializer.deserializeFromStream(type.toClass(), in);
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeString(name);
        out.writeInt(data.getType().toInt());
        out.writeSerializable(data);
    }
}
