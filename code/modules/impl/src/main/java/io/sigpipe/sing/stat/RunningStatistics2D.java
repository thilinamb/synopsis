/*
Copyright (c) 2014, Colorado State University
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

import java.io.IOException;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

import org.apache.commons.math3.special.Beta;

/**
 * Expanding on the {@link RunningStatistics} class, this class supports
 * two dimensions (x and y), along with some extra features (simple linear
 * regression, r^2, calculating correlation).
 *
 * @author malensek
 */
public class RunningStatistics2D implements ByteSerializable {

    private RunningStatistics xs;
    private RunningStatistics ys;

    private double SSxy;

    /**
     * Encapsulates the results of a Pearson product-moment correlation
     * coefficient calculation, including the two-tailed p-value.
     */
    public class PearsonResult {
        /** Pearson's r-value */
        double r;

        /** p-value */
        double p;

        public PearsonResult(double r, double p) {
            this.r = r;
            this.p = p;
        }
    }

    public RunningStatistics2D() {
        xs = new RunningStatistics();
        ys = new RunningStatistics();
    }

    public void merge(RunningStatistics2D that) {
        long thisN = this.n();
        long thatN = that.n();
        RunningStatistics thatX = that.xStat();
        RunningStatistics thatY = that.yStat();
        double xDelta = thatX.mean() - this.xs.mean();
        double yDelta = thatY.mean() - this.ys.mean();

        this.SSxy = this.SSxy() + that.SSxy()
            + thisN * thatN * xDelta * yDelta / (thisN + thatN);

        this.xs.merge(thatX);
        this.ys.merge(thatY);
    }

    /**
     * Adds several new (x, y) sample pairs to the 2D running statistics.
     *
     * @param samples Array of two-element arrays (representing x and y)
     */
    public void put(double[]... samples) {
        for (double[] sample : samples) {
            if (sample.length != 2) {
                throw new IllegalArgumentException("Input arrays must contain "
                        + "a single sample pair (x, y)");
            }
            put(sample[0], sample[1]);
        }
    }

    /**
     * Adds a new sample to the 2D running statistics.
     */
    public void put(double x, double y) {
        double dx = x - xs.mean();
        double dy = y - ys.mean();
        SSxy += dx * dy * n() / (n() + 1);

        xs.put(x);
        ys.put(y);
    }

    /**
     * @return Sum of the cross products (xy)
     */
    public double SSxy() {
        return SSxy;
    }

    /**
     * @return Sum of squared deviations from the mean of x
     */
    public double SSxx() {
        return xs.var() * (n() - 1.0);
    }

    /**
     * @return Sum of squared deviations from the mean of y
     */
    public double SSyy() {
        return ys.var() * (n() - 1.0);
    }

    /**
     * Calculate the Pearson product-moment correlation coefficient.
     *
     * @return PPMCC (Pearson's r)
     */
    public double r() {
        double r = Math.sqrt(r2());
        if (r > 1.0) {
            r = 1.0;
        }
        return r;
    }

    /**
     * Calculate the Pearson product-moment correlation coefficient, including
     * the two-tailed p-value.
     *
     * @return PearsonResult containing the r-value and two-tailed p-value.
     */
    public PearsonResult rp() {
        double r = r();
        double p = 0.0;
        if (Math.abs(r) != 1.0) {
            double df = n() - 2;
            double t2 = r * r * (df / ((1.0 - r) * (1.0 + r)));
            p = Beta.regularizedBeta(df / (df + t2), 0.5 * df, 0.5);
        }
        return new PearsonResult(r, p);
    }

    /**
     * Calculate the coefficient of determination (r squared).
     *
     * @return coefficient of determination
     */
    public double r2() {
        double SSE = (SSyy() - SSxy() * SSxy() / SSxx());
        return (SSyy() - SSE) / SSyy();
    }

    /**
     * @return the slope of the regression line (beta).
     */
    public double slope() {
        return SSxy() / SSxx();
    }

    /**
     * @return the y-intercept of the regression line (alpha).
     */
    public double intercept() {
        return ys.mean() - slope() * xs.mean();
    }

    /**
     * Given a value of x, estimate the outcome of y using simple linear
     * regression.
     *
     * @return estimated value of y
     */
    public double predict(double x) {
        return intercept() + slope() * x;
    }

    /**
     * @return a copy of the running statistics instance for x.
     */
    public RunningStatistics xStat() {
        return new RunningStatistics(xs);
    }

    /**
     * @return a copy of the running statistics instance for y.
     */
    public RunningStatistics yStat() {
        return new RunningStatistics(ys);
    }

    /**
     * Retrieves the number of samples submitted to the RunningStatistics2D
     * instance so far.
     *
     * @return number of samples
     */
    public long n() {
        return xs.n();
    }

    @Deserialize
    public RunningStatistics2D(SerializationInputStream in)
    throws IOException {
        xs = new RunningStatistics(in);
        ys = new RunningStatistics(in);
        SSxy = in.readDouble();
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeSerializable(xs);
        out.writeSerializable(ys);
        out.writeDouble(SSxy);
    }
}
