package io.sigpipe.sing.stat;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;

/**
 * Creates synthetic data points using a populated RunningStatisticsND instance
 * by sampling from a representative distribution.
 */
public class SyntheticDataND {

    private RealDistribution[] distributions;
    private double[] mins;
    private double[] maxes;
    private double[] means;
    private boolean[] isBoolean;

    public SyntheticDataND(RunningStatisticsND stats) {
        this.mins = stats.mins();
        this.maxes = stats.maxes();
        this.means = stats.means();
        double[] stds = stats.stds();

        this.distributions = new NormalDistribution[stats.dimensions()];
        for (int i = 0; i < stats.dimensions(); ++i) {
            NormalDistribution nd = new NormalDistribution(means[i], stds[i]);
            distributions[i] = nd;
        }

        this.isBoolean = new boolean[stats.dimensions()];
    }

    /**
     * Enables or disables the boolean flag for the specified dimension (values
     * of 0.0 or 1.0 only). Booleans are handled differently by the synthetic
     * data generation process.
     */
    public void setBoolean(int dimension, boolean isBoolean) {
        this.isBoolean[dimension] = isBoolean;
    }

    /**
     * Flags the specified dimension as a boolean variable (values of 0.0 or 1.0
     * only). Booleans are handled differently by the synthetic data generation
     * process.
     */
    public void setBoolean(int dimension) {
        setBoolean(dimension, true);
    }

    /**
     * Retrieves the number of dimensions managed by this SyntheticDataND
     * instance.
     */
    public int dimensions() {
        return distributions.length;
    }

    /**
     * Retrieves the next set of samples for each dimension.
     */
    public double[] nextSample() {
        double[] samples = new double[this.dimensions()];
        for (int i = 0; i < samples.length; ++i) {
            samples[i] = nextSample(i);
        }
        return samples;
    }

    /**
     * Retrieves the next sample for a particular dimension.
     */
    public double nextSample(int dimension) {
        if (isBoolean[dimension]) {
            return nextBooleanSample(dimension);
        }

        while (true) {
            double sample = distributions[dimension].sample();
            if (sample <= maxes[dimension] && sample >= mins[dimension]) {
                return sample;
            }
        }
    }

    /**
     * Retrieves the next sample from a dimension that is boolean (0.0 or 1.0
     * only).
     */
    private double nextBooleanSample(int dimension) {
        double sample = distributions[dimension].sample();

        if (sample < 0.5) {
            return 0.0;
        } else if (sample > 0.5) {
            return 1.0;
        }

        /* When the sampled value is exactly 0.5, we bias towards 0.0 here */
        return 0.0;
    }

    public static void main(String[] args) throws Exception {
        RunningStatisticsND rnd = new RunningStatisticsND(1);
        Files.lines(Paths.get(args[0]))
            .map(Double::parseDouble)
            .forEach(item -> rnd.put(item));

        SyntheticDataND sd = new SyntheticDataND(rnd);
        //sd.setBoolean(0);
        RunningStatistics rs = new RunningStatistics();
        for (int i = 0; i < 10000; ++i) {
            double d = sd.nextSample(0);
            rs.put(d);
        }
        System.out.println(rs);

        System.out.println(rnd.mean(0));
        System.out.println(rnd.std(0));
    }
}
