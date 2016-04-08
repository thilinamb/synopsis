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

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationException;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages a multidimensional array of {@link Feature} instances.
 *
 * @author malensek
 */
public class FeatureArray implements ByteSerializable {

    private static final Logger logger = Logger.getLogger("io.sigpipe.sing");

    private int[] dimensions;
    private int[] offsets;

    private List<Feature> features;

    private boolean named;
    private String name;

    private boolean typed;
    private FeatureType type;

    /**
     * Creates a new FeatureArray with the specified dimensions and a specific
     * {@link FeatureType} and Feature name.  A FeatureArray created with these
     * parameters will not allow variable FeatureTypes to be inserted into the
     * array, and will not allow variable Feature names to be used.  Specifying
     * the Feature name and FeatureType ahead of time reduces the amount of
     * information tracked by the array, as well as the amount of information
     * that must be serialized.
     *
     * @param dimensions A list of dimensions.  For example, passing {10} would
     * create a 1D array with 10 elements.  Passing {10, 100} would create a 2D
     * array with a total of 1000 elements.
     */
    public FeatureArray(String name, FeatureType type, int... dimensions) {
        this(type, dimensions);

        this.named = true;
        this.name = name;
    }

    /**
     * Creates a new FeatureArray with the specified dimensions and a specific
     * {@link FeatureType}.  A FeatureArray created with these parameters will
     * not allow variable FeatureTypes to be inserted into the array.
     *
     * @param dimensions A list of dimensions.  For example, passing {10} would
     * create a 1D array with 10 elements.  Passing {10, 100} would create a 2D
     * array with a total of 1000 elements.
     */
    public FeatureArray(FeatureType type, int... dimensions) {
        this(dimensions);

        this.typed = true;
        this.type = type;
    }

    /**
     * Creates a new FeatureArray with the specified dimensions.  FeatureArrays
     * created in this way will allow variable names and FeatureTypes of its
     * children Features, which offers the most flexibility but also consumes
     * the most memory and/or serialization processing time.
     *
     * @param dimensions A list of dimensions.  For example, passing {10} would
     * create a 1D array with 10 elements.  Passing {10, 100} would create a 2D
     * array with a total of 1000 elements.
     */
    public FeatureArray(int... dimensions) {
        constructBackingStore(dimensions);
    }

    /**
     * Creates a flat (1D) array that will be used to store Feature values for
     * the given dimensions.
     *
     * @param dimensions The dimensions of the array; for example, (10, 30)
     * would produce a 2D array of size 10x30.
     */
    private void constructBackingStore(int... dimensions) {
        this.dimensions = dimensions;

        /* Determine the overall size of the (collapsed) array */
        int size = 1;
        for (int i = 0; i < dimensions.length; ++i) {
            if (dimensions[i] <= 0) {
                throw new IllegalArgumentException("Invalid array dimension");
            }
            size = size * dimensions[i];
        }
        Feature nullFeature = new Feature();
        features = new ArrayList<>(Collections.nCopies(size, nullFeature));
        calculateOffsets(dimensions);
    }

    /**
     * Determines array offsets given the specified dimensions.
     */
    private void calculateOffsets(int... dimensions) {
        offsets = new int[dimensions.length];
        for (int i = 0; i < dimensions.length; ++i) {
            offsets[i] = 1;
            for (int j = i + 1; j < dimensions.length; ++j) {
                offsets[i] = offsets[i] * dimensions[j];
            }
        }
    }

    /**
     * Creates a FeatureArray from a Java array.  The type of the array is
     * ascertained from the Java type.
     *
     * @param name Name of the Features in this FeatureArray
     * @param features Java array containing feature values
     */
    public FeatureArray(String name, Object features) {
        this.named = true;
        this.name = name;
        this.typed = true;

        int[] dimensions = getMaxDimensions(features);
        constructBackingStore(dimensions);
        PrimitiveArray(features);
    }

    /**
     * Converts a ND native array to a FeatureArray.
     *
     * @param array multidimensional native array to convert.
     */
    private void PrimitiveArray(Object array) {
        AtomicInteger counter = new AtomicInteger();
        convertPrimitiveArray(counter, array);
    }

    /**
     * Recursive method for converting a native array to a FeatureArray.  This
     * method scans through the native array, creating Feature instances and
     * populating the FeatureArray as it goes.
     *
     * @param counter used to track the 1D array indices as they are populated
     * @param array multidimensional native array to convert
     */
    private void convertPrimitiveArray(AtomicInteger counter, Object array) {
        try {
            Array.getLength(array);
        } catch (Exception e) {
            /* Not an array */
            int index = counter.getAndIncrement();
            Feature feature;
            try {
                feature = Feature.fromPrimitiveType(array);

                if (this.type == null) {
                    this.type = feature.getType();
                }
            } catch (NullPointerException npe) {
                /* A null pointer here means that there was nothing in the array
                 * that was passed in, so we convert it to a NullFeature. */
                feature = new Feature();
            }
            features.set(index, feature);
            return;
        }

        Object[] castedArray = ((Object[]) array);
        for (int i = 0; i < castedArray.length; ++i) {
            convertPrimitiveArray(counter, castedArray[i]);
        }
    }

    /**
     * Determines the largest dimensions of a multidimensional Java array --
     * this is useful because Java arrays can be jagged, whereas Galileo arrays
     * are not.
     *
     * @param features Multidimensional java array of features
     *
     * @return Maximum dimensions of the array passed in
     */
    private int[] getMaxDimensions(Object features) {
        List<Integer> maxes = new ArrayList<>();
        getMaxDimensions(0, maxes, features);

        int[] dimensions = new int[maxes.size()];
        for (int i = 0; i < maxes.size(); ++i) {
            dimensions[i] = maxes.get(i);
        }
        return dimensions;
    }

    /**
     * Recursive method to scan for the largest dimensions in the array.  Since
     * Java arrays can be jagged, each element's size must be checked.
     *
     * @param level Current dimension being scanned (0 = first dim of array)
     * @param maxes The largest sized array seen so far, for each level.  This
     * is updated as the scan takes place.
     * @param features Feature arrays to scan
     */
    private void getMaxDimensions(
            int level, List<Integer> maxes, Object features) {
        int length;
        try {
            length = Array.getLength(features);
        } catch (Exception e) {
            /* Not an array */
            return;
        }

        if (maxes.size() < level + 1) {
            maxes.add(length);
        } else if (maxes.get(level) < length) {
            maxes.set(level, length);
        }

        Object[] castedFeatures = (Object[]) features;
        for (int i = 0; i < castedFeatures.length; ++i) {
            getMaxDimensions(level + 1, maxes, castedFeatures[i]);
        }
    }

    /**
     * Converts multidimensional array indices into the raw 1D array index used
     * to represent the array.
     *
     * @param indices list of multidimensional array indices.
     *
     * @return raw index corresponding to the indices in the backing store.
     */
    private int getIndex(int... indices) {
        if (indices.length != dimensions.length) {
            throw new IllegalArgumentException("Index array must match "
                    + "array rank.");
        }
        checkIndexBounds(indices);

        int index = 0;
        for (int i = 0; i < indices.length; ++i) {
            index = index + offsets[i] * indices[i];
        }
        return index;
    }

    /**
     * Ensures that the provided indices are not out of bounds.
     */
    private void checkIndexBounds(int... indices) {
        for (int i = 0; i < dimensions.length; ++i) {
            if (indices[i] >= dimensions[i]) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    /**
     * Retrieves the Feature at the provided indices.
     */
    public Feature get(int... indices) {
        int index = getIndex(indices);
        Feature feature = features.get(index);
        if (this.named == false) {
            return feature;
        } else {
            return new Feature(this.name, feature);
        }
    }

    /**
     * Sets the value of the Feature at the provided indices.
     */
    public void set(Feature feature, int... indices) {
        if (this.typed) {
            if (feature.getType() != this.type) {
                throw new IllegalArgumentException("FeatureType mismatch. "
                        + "Array Type: " + this.type + "; "
                        + "Feature Type: " + feature.getType());
            }
        }
        int index = getIndex(indices);
        features.set(index, feature);
    }

    /**
     * Clears the Feature at the provided indices.  This would be the same as
     * setting the array element to a NullFeatureData instance.
     */
    public void erase(int... indices) {
        int index = getIndex(indices);
        features.set(index, new Feature());
    }

    public boolean isNamed() {
        return this.named;
    }

    public String getName() {
        return this.name;
    }

    public boolean isTyped() {
        return this.typed;
    }

    public FeatureType getType() {
        return this.type;
    }

    /**
     * Retrieves the rank (dimensionality) of this array.
     */
    public int getRank() {
        return dimensions.length;
    }

    /**
     * Gets the size of the dimensions in this array.
     */
    public int[] getDimensions() {
        return dimensions;
    }

    /**
     * Retrieves the overall size of the (flattened) array.
     */
    public int getSize() {
        return features.size();
    }

    @Deserialize
    public FeatureArray(SerializationInputStream in)
    throws IOException {
        this.named = in.readBoolean();
        if (this.named) {
            this.name = in.readString();
        }

        this.typed = in.readBoolean();
        if (this.typed) {
            this.type = FeatureType.fromInt(in.readInt());
        }

        int rank = in.readInt();
        dimensions = new int[rank];
        for (int i = 0; i < rank; ++i) {
            dimensions[i] = in.readInt();
        }
        calculateOffsets(dimensions);

        int numFeatures = in.readInt();
        features = new ArrayList<>(numFeatures);
        Feature nullFeature = new Feature();
        for (int i = 0; i < numFeatures; ++i) {
            Feature feature = nullFeature;
            try {
                feature = new Feature(in);
            } catch (SerializationException e) {
                logger.log(Level.WARNING, "Error deserializing FeatureArray "
                        + "element", e);
            }
            features.add(feature);
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeBoolean(this.named);
        if (named) {
            out.writeString(this.name);
        }

        out.writeBoolean(this.typed);
        if (typed) {
            out.writeInt(this.type.toInt());
        }

        out.writeInt(getRank());
        for (int dim : dimensions) {
            out.writeInt(dim);
        }

        out.writeInt(features.size());
        for (Feature feature : features) {
            out.writeSerializable(feature);
        }
    }
}
