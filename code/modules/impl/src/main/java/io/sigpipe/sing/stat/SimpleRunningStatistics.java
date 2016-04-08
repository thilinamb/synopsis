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

package io.sigpipe.sing.stat;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

import java.io.IOException;

/**
 * Provides an online method for computing mean, variance, and standard
 * deviation.  Based on "Note on a Method for Calculating Corrected Sums of
 * Squares and Products" by B. P. Welford.
 *
 * @author malensek
 */
public class SimpleRunningStatistics implements ByteSerializable {

    private long n;
    private double mean;
    private double m2;

    /**
     * Creates an empty running statistics instance.
     */
    public SimpleRunningStatistics() {

    }

    /**
     * Creates a running statistics instance with an array of samples.
     * Samples are added to the statistics in order.
     */
    public SimpleRunningStatistics(double... samples ) {
        for (double sample : samples) {
            put(sample);
        }
    }

    /**
     * Creates a copy of a {@link SimpleRunningStatistics} instance.
     */
    public SimpleRunningStatistics(SimpleRunningStatistics that) {
        copyFrom(that);
    }

    /**
     * Create a new {@link SimpleRunningStatistics} instance by combining
     * multiple existing instances.
     */
    public SimpleRunningStatistics(SimpleRunningStatistics... others) {
        if (others.length == 0) {
            return;
        } else if (others.length == 1) {
            copyFrom(others[0]);
            return;
        }

        /* Calculate new n */
        for (SimpleRunningStatistics rs : others) {
            merge(rs);
        }
    }

    /**
     * Copies statistics from another SimpleRunningStatistics instance.
     */
    private void copyFrom(SimpleRunningStatistics that) {
        this.n = that.n;
        this.mean = that.mean;
        this.m2 = that.m2;
    }

    /**
     * Merges this set of running statistics with another.
     */
    public void merge(SimpleRunningStatistics that) {
        long newN = n + that.n;
        double delta = this.mean - that.mean;
        mean = (this.n * this.mean + that.n * that.mean) / newN;
        m2 = m2 + that.m2 + delta * delta * this.n * that.n / newN;
        n = newN;
    }

    /**
     * Add multiple new samples to the running statistics.
     */
    public void put(double... samples) {
        for (double sample : samples) {
            put(sample);
        }
    }

    /**
     * Add a new sample to the running statistics.
     */
    public void put(double sample) {
        n++;
        double delta = sample - mean;
        mean = mean + delta / n;
        m2 = m2 + delta * (sample - mean);
    }

    /**
     * Removes a previously-added sample from the running statistics. WARNING:
     * give careful consideration when using this method. If a value is removed
     * that wasn't previously added, the statistics will be meaningless.
     * Additionally, if you're keeping track of previous additions, then it
     * might be worth evaluating whether a SimpleRunningStatistics instance is
     * the right thing to be using at all. Caveat emptor, etc, etc.
     */
    public void remove(double sample) {
        if (n <= 1) {
            /* If we're removing the last sample, then just clear the stats. */
            clear();
            return;
        }

        double prevMean = (n * mean - sample) / (n - 1);
        m2 = m2 - (sample - mean) * (sample - prevMean);
        mean = prevMean;
        n--;
    }

    /**
     * Clears all values passed in, returning the SimpleRunningStatistics
     * instance to its original state.
     */
    public void clear() {
        n = 0;
        mean = 0;
        m2 = 0;
    }

    /**
     * Calculates the current running mean for the values observed thus far.
     *
     * @return mean of all the samples observed thus far.
     */
    public double mean() {
        return mean;
    }

    /**
     * Calculates the running sample variance.
     *
     * @return sample variance
     */
    public double var() {
        return var(1.0);
    }

    /**
     * Calculates the population variance.
     *
     * @return population variance
     */
    public double popVar() {
        return var(0.0);
    }

    /**
     * Calculates the running variance, given a bias adjustment.
     *
     * @param ddof delta degrees-of-freedom to use in the calculation.  Use 1.0
     * for the sample variance.
     *
     * @return variance
     */
    public double var(double ddof) {
        if (n == 0) {
            return Double.NaN;
        }

        return m2 / (n - ddof);
    }

    /**
     * Calculates the standard deviation of the data observed thus far.
     *
     * @return sample standard deviation
     */
    public double std() {
        return Math.sqrt(var());
    }

    /**
     * Calculates the sample standard deviation of the data observed thus
     * far.
     *
     * @return population standard deviation
     */
    public double popStd() {
        return Math.sqrt(popVar());
    }

    /**
     * Calculates the standard deviation of the values observed thus far, given
     * a bias adjustment.
     *
     * @param ddof delta degrees-of-freedom to use in the calculation.
     *
     * @return standard deviation
     */
    public double std(double ddof) {
        return Math.sqrt(var(ddof));
    }

    /**
     * Retrieves the number of samples submitted to the SimpleRunningStatistics
     * instance so far.
     *
     * @return number of samples
     */
    public long count() {
        return n;
    }

    @Override
    public String toString() {
        String str = "";
        str += "Number of Samples: " + n + System.lineSeparator();
        str += "Mean: " + mean + System.lineSeparator();
        str += "Variance: " + var() + System.lineSeparator();
        str += "Std Dev: " + std() + System.lineSeparator();
        return str;
    }

    @Deserialize
    public SimpleRunningStatistics(SerializationInputStream in)
    throws IOException {
        n = in.readLong();
        mean = in.readDouble();
        m2 = in.readDouble();
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeLong(n);
        out.writeDouble(mean);
        out.writeDouble(m2);
    }
}
