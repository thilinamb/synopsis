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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.analysis.integration.SimpsonIntegrator;

import io.sigpipe.sing.adapters.ReadMetadata;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.stat.OnlineKDE;
import io.sigpipe.sing.stat.SquaredError;
import io.sigpipe.sing.stat.SummaryStatistics;
import io.sigpipe.sing.util.TestConfiguration;

/**
 * Given a list of Feature values or a predefined {@link OnlineKDE} instance,
 * the AutoQuantizer attempts to spread a given number of tick marks across a
 * distribution such that the densest parts of the curve are represented by the
 * smallest tick mark ranges.
 *
 * @author malensek
 */
public class AutoQuantizer {

    public static Quantizer fromKDE(OnlineKDE kde, int ticks) {
        return fromKDE(kde, ticks, true);
    }

    public static Quantizer fromKDE(
            OnlineKDE kde, int ticks, boolean expandRange) {

        SimpsonIntegrator integrator = new SimpsonIntegrator();
        double start = 0.0;
        double end = 0.0;

        /** Fraction of the PDF to allocate to each tick */
        double tickSize = 1.0 / (double) ticks;

        if (expandRange) {
            start = kde.expandedMin();
            end = kde.expandedMax();
        } else {
            SummaryStatistics ss = kde.summaryStatistics();
            start = ss.min();
            start = ss.max();
        }

        double step = ((end - start) / (double) ticks) * 0.01;
        List<Feature> tickList = new ArrayList<>();
        for (int t = 0; t < ticks; ++t) {
            double total = 0.0;
            tickList.add(new Feature(start));
            double increment = step;
            while (total < tickSize) {
                double integral = integrator.integrate(
                        Integer.MAX_VALUE, kde, start, start + increment);
                if (total + integral > (tickSize * 1.05)) {
                    //System.err.println("Oversized: " + t + " ; " + total + " + " + integral + " [" + tickSize + "]");
                    increment = increment / 2.0;
                    continue;
                }

                total += integral;
                start = start + increment;
                if (start > end) {
                    break;
                }
            }
        }
        tickList.add(new Feature(start));

        return new Quantizer(tickList);
    }

    public static Quantizer fromList(List<Feature> features, int ticks) {
        return fromList(features, ticks, true);
    }

    public static Quantizer fromList(
            List<Feature> features, int ticks, boolean expandRange) {
        /* Seed the oKDE */
        int seedSize = 1000;
        List<Double> seedValues = new ArrayList<>();
        for (int i = 0; i < seedSize; ++i) {
            seedValues.add(features.get(i).getDouble());
        }
        OnlineKDE kde = new OnlineKDE(seedValues);

        /* Populate the rest of the data */
        for (int i = seedSize; i < features.size(); i += 50) {
            kde.updateDistribution(features.get(i).getDouble());
        }

        return AutoQuantizer.fromKDE(kde, ticks, expandRange);
    }

    public static void main(String[] args)
        throws Exception {
        for (String name : TestConfiguration.FEATURE_NAMES) {
            List<Feature> features = new ArrayList<>();

            for (String fileName : args) {
                System.err.println("Reading: " + fileName);
                List<Metadata> meta = ReadMetadata.readMetaBlob(new File(fileName));
                for (Metadata m : meta) {
                    Feature f = m.getAttribute(name);
                    if (f != null) {
                        features.add(m.getAttribute(name));
                    } else {
                        System.err.println("null feature: " + name);
                    }
                }
            }

            Quantizer q = null;
            int ticks = 10;
            double err = Double.MAX_VALUE;
            while (err > 0.025) {
                q = AutoQuantizer.fromList(features, ticks);
                //System.out.println(q);

                List<Feature> quantized = new ArrayList<>();
                for (Feature f : features) {
                    /* Find the midpoint */
                    Feature initial = q.quantize(f.convertTo(FeatureType.DOUBLE));
                    Feature next = q.nextTick(initial);
                    if (next == null) {
                        next = initial;
                    }
                    Feature difference = next.subtract(initial);
                    Feature midpoint = difference.divide(new Feature(2.0f));
                    Feature prediction = initial.add(midpoint);

                    quantized.add(prediction);

                    //System.out.println(f.getFloat() + "    " + predicted.getFloat());
                }

                SquaredError se = new SquaredError(features, quantized);
                System.out.println(name + "    " + q.numTicks() + "    " + se.RMSE() + "    "
                        + se.NRMSE() + "    " + se.CVRMSE());
                err = se.NRMSE();
                ticks += 1;
            }
            System.out.println(q);
        }
    }
}
