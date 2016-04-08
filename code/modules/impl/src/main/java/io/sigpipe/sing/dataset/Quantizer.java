/*
Copyright (c) 2016, Colorado State University
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

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import io.sigpipe.sing.dataset.feature.Feature;

/**
 * Handles quantization of {@link Feature} values. In other words, a Quantizer
 * instance takes in high-resolution data and outputs low-resolution values that
 * represent subdivisions ("buckets" or "tick marks") in the feature space. Each
 * bucket handles Features for a particular range of values.
 * <p>
 * For example, imagine a Quantizer instance set up for values ranging from 0 to
 * 100, with a step size of 2 (note that each of these values are integers).
 * Inputting values of 24.6f or 25.9999f would return bucket 24, while 99.9d
 * would return 98, and 1 would return 0.
 * <p>
 * Note that the outputs returned by the Quantizer.quantize() method are not
 * meant to be 'closest' to their original values; rather, the output values are
 * simply identifiers for the particular bucket in question.
 *
 * @author malensek
 */
public class Quantizer {

    private NavigableSet<Feature> ticks = new TreeSet<>();

    /**
     * Constructs a Quantizer with a predefined list of Tick marks. For
     * incremental construction of a Quantizer instance, see
     * {@link QuantizerBuilder}.
     *
     * @param ticks collection of tick marks to be used during quantization
     */
    public Quantizer(Feature... ticks) {
        for (Feature tick : ticks) {
            addTick(tick);
        }
    }

    /**
     * Constructs a Quantizer with a predefined collection of Tick marks. For
     * incremental construction of a Quantizer instance, see
     * {@link QuantizerBuilder}.
     *
     * @param ticks collection of tick marks to be used during quantization
     */
    public Quantizer(Iterable<Feature> ticks) {
        for (Feature tick : ticks) {
            addTick(tick);
        }
    }

    /**
     * Constructs a Quantizer with start and end points, as well as an
     * intermediate step size that is used to populate tick marks uniformly
     * between. Primitive types may be used to parameterize this constructor,
     * but note that each parameter must be of the same type to avoid ambiguity
     * during quantization.
     *
     * @param start The beginning of the feature range
     * @param end The end of the feature range
     * @param step Step size to use for populating intermediate tick marks
     */
    public Quantizer(Object start, Object end, Object step) {
        this(
                Feature.fromPrimitiveType(start),
                Feature.fromPrimitiveType(end),
                Feature.fromPrimitiveType(step));
    }

    /**
     * Constructs a Quantizer with start and end points, as well as an
     * intermediate step size that is used to populate tick marks uniformly
     * between. Note that each of these features must be of the same
     * {@link FeatureType} to avoid ambiguity during quantization.
     *
     * @param start The beginning of the feature range
     * @param end The end of the feature range
     * @param step Step size to use for populating intermediate tick marks
     */
    public Quantizer(Feature start, Feature end, Feature step) {
        if (start.sameType(end) == false || start.sameType(step) == false) {
            throw new IllegalArgumentException(
                    "All feature types must be the same");
        }

        Feature tick = new Feature(start);
        while (tick.less(end)) {
            addTick(tick);
            tick = tick.add(step);
        }
    }

    /**
     * Adds a new tick mark value to this Quantizer.
     *
     * @param tick the new tick mark to add
     */
    private void addTick(Feature tick) {
        ticks.add(tick);
    }

    /**
     * Retrieves the number of tick mark subdivisions in this Quantizer.
     *
     * @return number of tick marks
     */
    public int numTicks() {
        return ticks.size();
    }

    /**
     * Quantizes a given Feature based on this Quantizer's tick mark
     * configuration. When quantizing a Feature, a bucket will be retrieved that
     * represents the Feature in question in the tick mark range. Note that the
     * bucket returned is not necessarily closest in value to the Feature, but
     * simply represents its range of values.
     *
     * @param feature The Feature to quantize
     * @return A quantized representation of the Feature
     */
    public Feature quantize(Feature feature) {
        Feature result = ticks.floor(feature);
        if (result == null) {
            return ticks.first();
        }

        return result;
    }

    /**
     * Retrieves the next tick mark value after the given Feature. In other
     * words, this method will return the bucket after the given Feature's
     * bucket. If there is no next tick mark (the specified Feature's bucket is
     * at the end of the range) then this method returns null.
     *
     * @param feature Feature to use to locate the next tick mark bucket in the
     *     range.
     * @return Next tick mark, or null if the end of the range has been reached.
     */
    public Feature nextTick(Feature feature) {
        return ticks.higher(feature);
    }

    /**
     * Retrieves the tick mark value preceding the given Feature. In other
     * words, this method will return the bucket before the given Feature's
     * bucket. If there is no previous tick mark (the specified Feature's bucket
     * is at the beginning of the range) then this method returns null.
     *
     * @param feature Feature to use to locate the previous tick mark bucket in
     *     the range.
     * @return Next tick mark, or null if the end of the range has been reached.
     */
    public Feature prevTick(Feature feature) {
        return ticks.lower(feature);
    }

    @Override
    public String toString() {
        String output = "";
        for (Feature f : ticks) {
            output += f.getString() + System.lineSeparator();
        }
        return output;
    }

    /**
     * Builder that allows incremental creation of a {@link Quantizer}.
     */
    public static class QuantizerBuilder {
        List<Feature> ticks = new ArrayList<>();

        public void addTick(Feature tick) {
            this.ticks.add(tick);
        }

        public void addTicks(Feature... ticks) {
            for (Feature tick : ticks) {
                addTick(tick);
            }
        }

        public void removeTick(Feature tick) {
            this.ticks.remove(tick);
        }

        public List<Feature> getTicks() {
            return new ArrayList<Feature>(ticks);
        }

        public Quantizer build() {
            Quantizer q = new Quantizer();
            for (Feature tick : ticks) {
                q.addTick(tick);
            }

            return q;
        }
    }
}
