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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.ejml.simple.SimpleMatrix;

import de.tuhh.luethke.okde.model.SampleModel;

public class OnlineKDE implements UnivariateFunction {

    private RunningStatistics stats = new RunningStatistics();
    private SampleModel model;

    private static final double DEFAULT_FORGET = 1.0d;
    private static final double DEFAULT_COMPRESSION = 0.02d;

    public OnlineKDE() {
        this(DEFAULT_FORGET, DEFAULT_COMPRESSION);
    }

    public OnlineKDE(double forgettingFactor, double compressionThreshold) {
        this.model = new SampleModel(forgettingFactor, compressionThreshold);
    }

    public OnlineKDE(List<Double> initialSamples) {
        this(initialSamples, DEFAULT_FORGET, DEFAULT_COMPRESSION);
    }

    public OnlineKDE(List<Double> initialSamples, double forgettingFactor,
            double compressionThreshold) {
        this(forgettingFactor, compressionThreshold);
        initializeDistribution(initialSamples);
    }

    private void initializeDistribution(List<Double> samples) {
        SimpleMatrix[] sampleMatrices = new SimpleMatrix[samples.size()];
        SimpleMatrix[] covarianceMatrices = new SimpleMatrix[samples.size()];
        double[] weights = new double[samples.size()];
        for (int i = 0; i < samples.size(); ++i) {
            double sample = samples.get(i);
            stats.put(sample);
            sampleMatrices[i] = new SimpleMatrix(
                    new double[][] { { sample } });
            covarianceMatrices[i] = new SimpleMatrix(1, 1);
            weights[i] = 1.0d;
        }
        try {
            this.model.updateDistribution(
                    sampleMatrices, covarianceMatrices, weights);
        } catch (Exception e) {
            //TODO generic online kde exception
            e.printStackTrace();
        }
    }

    public void updateDistribution(double sample) {
        stats.put(sample);
        SimpleMatrix mat = new SimpleMatrix(new double[][] { { sample } });
        SimpleMatrix cov = new SimpleMatrix(1, 1);
        try {
            this.model.updateDistribution(mat, cov, 1.0d);
        } catch (Exception e) {
            //TODO generic online kde exception
            e.printStackTrace();
        }
    }

    public void updateDistribution(Double... samples) {
        updateDistribution(Arrays.asList(samples));
    }

    public void updateDistribution(Iterable<Double> samples) {
        for (double d : samples) {
            updateDistribution(d);
        }
    }

    public double value(double x) {
        return this.model.evaluate(new SimpleMatrix(new double[][] { { x } }));
    }

    public double expandedMin() {
        double val = stats.min();
        while (this.model.evaluate(
                    new SimpleMatrix(
                        new double[][] { { val } })) != 0.0) {
            val = val - 0.1;
        }

        return val;
    }

    public double expandedMax() {
        double val = stats.min();
        while (this.model.evaluate(
                    new SimpleMatrix(
                        new double[][] { { val } })) != 0.0) {
            val = val + 0.1;
        }
        return val;
    }

    public SummaryStatistics summaryStatistics() {
        return new SummaryStatistics(stats);
    }

    public String toString(double step) {
        String str = "";
        double min = expandedMin();
        double max = expandedMax();
        for (double i = min; i <= max; i += step) {
            double[][] point = { { i } };
            SimpleMatrix pointVector = new SimpleMatrix(point);
            str += i + "\t" + this.model.evaluate(pointVector)
                + System.lineSeparator();
        }
        return str;
    }

    @Override
    public String toString() {
        return toString(1.0d);
    }
}
