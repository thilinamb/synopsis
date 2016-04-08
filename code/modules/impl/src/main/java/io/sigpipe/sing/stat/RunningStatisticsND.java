package io.sigpipe.sing.stat;

import java.io.IOException;

import org.apache.commons.math3.util.FastMath;

import io.sigpipe.sing.serialization.ByteSerializable;
import io.sigpipe.sing.serialization.SerializationInputStream;
import io.sigpipe.sing.serialization.SerializationOutputStream;

public class RunningStatisticsND implements ByteSerializable {

    private long n;

    private double[] mean;
    private double[] m2;
    private double[] min;
    private double[] max;

    private double[] ss;

    public RunningStatisticsND() {

    }

    public RunningStatisticsND(int dimensions) {
        this.initialize(dimensions);
    }

    public RunningStatisticsND(double... samples) {
        this(samples.length);
        put(samples);
    }

    public RunningStatisticsND(RunningStatisticsND that) {
        this.copyFrom(that);
    }

    /**
     * Initializes all instance variables based on a given number of dimensions.
     * Useful for constructing new instances or resetting already existing
     * instances.
     *
     * @param dimensions Number of dimensions to initialize
     */
    private void initialize(int dimensions) {
        this.n = 0;

        this.mean = new double[dimensions];
        this.m2 = new double[dimensions];
        this.min = new double[dimensions];
        this.max = new double[dimensions];

        for (int d = 0; d < dimensions; ++d) {
            this.min[d] = Double.MAX_VALUE;
            this.max[d] = Double.MIN_VALUE;
        }

        this.ss = new double[dimensions * (dimensions - 1) / 2];
    }

    private boolean initialized() {
        return mean != null;
    }

    private void copyFrom(RunningStatisticsND that) {
        if (that.initialized() == false) {
            return;
        }

        initialize(that.dimensions());
        this.n = that.n;
        for (int i = 0; i < that.dimensions(); ++i) {
            this.mean[i] = that.mean[i];
            this.m2[i] = that.m2[i];
            this.min[i] = that.min[i];
            this.max[i] = that.max[i];
        }
        for (int i = 0; i < that.ss.length; ++i) {
            this.ss[i] = that.ss[i];
        }
    }

    /**
     * Converts a 2D matrix index (i, j) to a 1D array position.
     *
     * @return corresponding array position.
     */
    private int index1D(int i, int j) {
        int dims = this.dimensions();
        return (dims * (dims - 1) / 2)
            - (dims - i) * ((dims - i) - 1) / 2 + j - i - 1;
    }

    /**
     * Add a new set of samples to the running statistics.
     */
    public void put(double... samples) {
        if (this.initialized() == false) {
            initialize(samples.length);
        }

        if (samples.length != this.dimensions()) {
            throw new IllegalArgumentException("Input dimension mismatch: "
                    + samples.length + " =/= " + this.dimensions());
        }

        n++;

        for (int i = 0; i < this.dimensions() - 1; ++i) {
            for (int j = i + 1; j < this.dimensions(); ++j) {
                double dx = samples[i] - mean[i];
                double dy = samples[j] - mean[j];
                int index = index1D(i, j);
                ss[index] += dx * dy * n / (n + 1);
            }
        }

        for (int d = 0; d < this.dimensions(); ++d) {
            double delta = samples[d] - mean[d];
            mean[d] = mean[d] + delta / n;
            m2[d] = m2[d] + delta * (samples[d] - mean[d]);

            min[d] = FastMath.min(min[d], samples[d]);
            max[d] = FastMath.max(max[d], samples[d]);
        }
    }

    public void merge(RunningStatisticsND that) {
        if (this.initialized() == false) {
            this.copyFrom(that);
            return;
        }

        if (this.dimensions() != that.dimensions()) {
            throw new IllegalArgumentException("Dimension mismatch: "
                    + this.dimensions() + " =/= " + that.dimensions() + "; "
                    + "merge operations require equal number of dimensions.");
        }

        long newN = n + that.n;

        for (int i = 0; i < this.dimensions() - 1; ++i) {
            for (int j = i + 1; j < this.dimensions(); ++j) {
                double dx = that.mean[i] - this.mean[i];
                double dy = that.mean[j] - this.mean[j];
                int index = index1D(i, j);
                ss[index] += that.ss[index] + this.n * that.n * dx * dy
                    / (this.n + that.n);
            }
        }

        for (int d = 0; d < this.dimensions(); ++d) {
            double delta = this.mean[d] - that.mean[d];
            this.mean[d] =
                (this.n * this.mean[d] + that.n * that.mean[d]) / newN;
            this.m2[d] += that.m2[d] + delta * delta * this.n * that.n / newN;

            min[d] = FastMath.min(this.min[d], that.min[d]);
            max[d] = FastMath.max(this.max[d], that.max[d]);
        }

        this.n = newN;
    }

    public void clear() {
        this.initialize(this.dimensions());
    }

    public int dimensions() {
        if (this.initialized() == false) {
            return 0;
        }

        return mean.length;
    }

    public long count() {
        return this.n;
    }

    public double mean(int dimension) {
        return this.mean[dimension];
    }

    public double max(int dimension) {
        return this.max[dimension];
    }

    @Deserialize
    public RunningStatisticsND(SerializationInputStream in)
    throws IOException {
        int dimensions = in.readInt();
        if (dimensions == 0) {
            return;
        }
        this.mean = new double[dimensions];
        this.m2 = new double[dimensions];
        this.min = new double[dimensions];
        this.max = new double[dimensions];
        this.ss = new double[dimensions * (dimensions - 1) / 2];

        this.n = in.readLong();

        for (int i = 0; i < dimensions; ++i) {
            mean[i] = in.readDouble();
            m2[i] = in.readDouble();
            min[i] = in.readDouble();
            max[i] = in.readDouble();
        }

        for (int i = 0; i < this.ss.length; ++i) {
            ss[i] = in.readDouble();
        }
    }

    @Override
    public void serialize(SerializationOutputStream out)
    throws IOException {
        out.writeInt(this.dimensions());
        if (this.dimensions() == 0) {
            return;
        }

        out.writeLong(n);

        for (int i = 0; i < this.dimensions(); ++i) {
            out.writeDouble(mean[i]);
            out.writeDouble(m2[i]);
            out.writeDouble(min[i]);
            out.writeDouble(max[i]);
        }

        for (int i = 0; i < ss.length; ++i) {
            out.writeDouble(ss[i]);
        }
    }

}
