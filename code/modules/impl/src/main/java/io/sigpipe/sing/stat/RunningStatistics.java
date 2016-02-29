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

package io.sigpipe.sing.stat;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

import java.io.IOException;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.util.FastMath;

/**
 * Provides an online method for computing mean, variance, and standard
 * deviation.  Based on "Note on a Method for Calculating Corrected Sums of
 * Squares and Products" by B. P. Welford.
 *
 * @author malensek
 */
public class RunningStatistics implements ByteSerializable {
    private long n;
    private double mean;
    private double M2;

    private double max;
    private double min;

    public static class WelchResult {
        /** T-statistic */
        public double t;

        /** Two-tailed p-value */
        public double p;

        public WelchResult(double t, double p) {
            this.t = t;
            this.p = p;
        }
    }

    /**
     * Creates a Welford running statistics instance without no observed values.
     */
    public RunningStatistics() { }

    /**
     * Creates a copy of a {@link RunningStatistics} instance.
     */
    public RunningStatistics(RunningStatistics that) {
        copyFrom(that);
    }

    /**
     * Create a new {@link RunningStatistics} instance by combining multiple
     * existing instances.
     */
    public RunningStatistics(RunningStatistics... others) {
        if (others.length == 0) {
            return;
        } else if (others.length == 1) {
            copyFrom(others[0]);
            return;
        }

        /* Calculate new n */
        for (RunningStatistics rs : others) {
            merge(rs);
        }
    }

    /**
     * Copies statistics from another RunningStatistics instance.
     */
    private void copyFrom(RunningStatistics that) {
        this.n = that.n;
        this.mean = that.mean;
        this.M2 = that.M2;
        this.max = that.max;
        this.min = that.min;
    }

    public void merge(RunningStatistics that) {
        long newN = n + that.n;
        double delta = this.mean - that.mean;
        mean = (this.n * this.mean + that.n * that.mean) / newN;
        M2 = M2 + that.M2 + delta * delta * this.n * that.n / newN;
        n = newN;
    }

    /**
     * Creates a Welford running statistics instance with an array of samples.
     * Samples are added to the statistics in order.
     */
    public RunningStatistics(double... samples ) {
        for (double sample : samples) {
            put(sample);
        }
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
        M2 = M2 + delta * (sample - mean);

        max = FastMath.max(this.max, sample);
        min = FastMath.min(this.min, sample);
    }

    /**
     * Removes a previously-added sample from the running statistics. WARNING:
     * give careful consideration when using this method. If a value is removed
     * that wasn't previously added, the statistics will be meaningless.
     * Additionally, if you're keeping track of previous additions, then it
     * might be worth evaluating whether a RunningStatistics instance is the
     * right thing to be using at all. Caveat emptor, etc, etc.
     */
    public void remove(double sample) {
        if (n <= 1) {
            /* If we're removing the last sample, then just clear the stats. */
            clear();
            return;
        }

        double prevMean = (n * mean - sample) / (n - 1);
        M2 = M2 - (sample - mean) * (sample - prevMean);
        mean = prevMean;
        n--;
    }

    /**
     * Clears all values passed in, returning the RunningStatistics instance to
     * its original state.
     */
    public void clear() {
        n = 0;
        mean = 0;
        M2 = 0;
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

        return M2 / (n - ddof);
    }

    /**
     * Calculates the standard deviation of the data observed thus far.
     *
     * @return sample standard deviation
     */
    public double std() {
        return FastMath.sqrt(var());
    }

    /**
     * Calculates the sample standard deviation of the data observed thus
     * far.
     *
     * @return population standard deviation
     */
    public double popStd() {
        return FastMath.sqrt(popVar());
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
        return FastMath.sqrt(var(ddof));
    }

    /**
     * Retrieves the largest value seen thus far by this RunningStatistics
     * instance.
     */
    public double max() {
        return this.max;
    }

    /**
     * Retrieves the smallest value seen thus far by this RunningStatistics
     * instance.
     */
    public double min() {
        return this.min;
    }

    public double prob(double sample) {
        double norm = 1 / FastMath.sqrt(2 * FastMath.PI * this.var());
        return norm * FastMath.exp((- FastMath.pow(sample - this.mean, 2))
                / (2 * this.var()));
    }

    /**
     * Retrieves the number of samples submitted to the RunningStatistics
     * instance so far.
     *
     * @return number of samples
     */
    public long n() {
        return n;
    }

    public static WelchResult welchT(
            RunningStatistics rs1, RunningStatistics rs2) {
        double vn1 = rs1.var() / rs1.n();
        double vn2 = rs2.var() / rs2.n();

        /* Calculate t */
        double xbs = rs1.mean() - rs2.mean();
        double t = xbs / FastMath.sqrt(vn1 + vn2);

        double vn12 = FastMath.pow(vn1, 2);
        double vn22 = FastMath.pow(vn2, 2);

        /* Calculate degrees of freedom */
        double v = FastMath.pow(vn1 + vn2, 2)
            / ((vn12 / (rs1.n() - 1)) + (vn22 / (rs2.n() - 1)));
        if (v == Double.NaN) {
            v = 1;
        }

        TDistribution tdist = new TDistribution(v);
        double p = tdist.cumulativeProbability(t) * 2;
        return new WelchResult(t, p);
    }

    @Override
    public String toString() {
        String str = "";
        str += "Number of Samples: " + n + System.lineSeparator();
        str += "Mean: " + mean + System.lineSeparator();
        str += "Variance: " + var() + System.lineSeparator();
        str += "Std Dev: " + std() + System.lineSeparator();
        str += "Min: " + min + System.lineSeparator();
        str += "Max: " + max;
        return str;
    }

    @Deserialize
    public RunningStatistics(SerializationInputStream in)
    throws IOException {
        n = in.readLong();
        mean = in.readDouble();
        M2 = in.readDouble();
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeLong(n);
        out.writeDouble(mean);
        out.writeDouble(M2);
    }
}
