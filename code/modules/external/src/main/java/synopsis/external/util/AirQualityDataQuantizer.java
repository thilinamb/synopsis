package synopsis.external.util;

import io.sigpipe.sing.dataset.AutoQuantizer;
import io.sigpipe.sing.dataset.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;
import io.sigpipe.sing.dataset.feature.FeatureType;
import io.sigpipe.sing.stat.SquaredError;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author Thilina Buddhika
 */
public class AirQualityDataQuantizer {
    private static class ReservoirSample {
        Feature[] reservoir;
        int memberCount;
        private Random random;
        private double min;
        private double max;

        private ReservoirSample(int size) {
            this.reservoir = new Feature[size];
            this.memberCount = 0;
            this.random = new Random(123);
            this.min = Double.MAX_VALUE;
            this.max = -1 * Double.MAX_VALUE;
        }

        private void addToReservoir(double v) {
            if (memberCount < reservoir.length) {
                reservoir[memberCount] = new Feature(v);
                memberCount++;
            } else {
                int rndInt = random.nextInt(reservoir.length);
                if (rndInt < reservoir.length) {
                    reservoir[rndInt] = new Feature(v);
                }
            }
            if (this.min > v) {
                this.min = v;
            }
            if (this.max < v) {
                this.max = v;
            }
        }

        private List<Feature> asList() {
            return Arrays.asList(reservoir);
        }
    }

    public static final int RESERVOIR_SIZE = 500000;

    public static void main(String[] args) {
        String baseDir = args[0];
        final String fileFilterStr = args[1];

        // filter out the list of files
        File[] inputFiles = new File(baseDir).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(fileFilterStr) && name.endsWith(".csv");
            }
        });

        System.out.println("Total File Count: " + inputFiles.length);

        ReservoirSample reservoirSample = new ReservoirSample(RESERVOIR_SIZE);
        for (File f : inputFiles) {
            FileReader fr = null;
            BufferedReader br = null;
            try {
                fr = new FileReader(f);
                br = new BufferedReader(fr);
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.startsWith("\"State Code\"")) {
                        double v = parse(line);
                        reservoirSample.addToReservoir(v);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading from file: " + f.getAbsolutePath());
                e.printStackTrace();
            } finally {
                try {
                    if (br != null) {
                        br.close();
                    }
                    if (fr != null) {
                        fr.close();
                    }
                } catch (IOException e) {
                    System.err.println("Errror closing input streams.");
                    e.printStackTrace();
                }
            }
        }

        // Following code was generously borrowed from Matthew's AutoQuantizer class
        Quantizer q = null;
        int ticks = 10;
        double err = Double.MAX_VALUE;
        while (err > 0.025) {
            q = AutoQuantizer.fromList(reservoirSample.asList(), ticks);
            //q = calculateGlobalEvenQuantizer(reservoirSample.min, reservoirSample.max, ticks);
            if (ticks == 10) {
                System.out.println(q);
            }

            List<Feature> quantized = new ArrayList<>();
            for (Feature f : reservoirSample.asList()) {
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

            SquaredError se = new SquaredError(reservoirSample.asList(), quantized);
            System.out.println(fileFilterStr + "    " + q.numTicks() + "    " + se.RMSE() + "    "
                    + se.NRMSE() + "    " + se.CVRMSE());
            err = se.NRMSE();
            ticks += 1;
        }
        System.out.println(q);
        try {
            FileWriter fw = new FileWriter("/tmp/" + fileFilterStr + ".q");
            fw.write(q.toString());
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double parse(String line) {
        try {
            String[] segments = line.split(",");
            return Double.parseDouble(segments[13]);
        } catch (Throwable e) {
            System.out.println("Problematic Line: " + line);
            System.out.println("Segment Count: " + line.split(",").length);
            e.printStackTrace();
            throw e;
        }
    }

    public static Quantizer calculateGlobalEvenQuantizer(double min, double max, int tickCount) {
        double range = max - min;
        double stepSize = range / (tickCount - 1);
        List<Feature> features = new ArrayList<>(tickCount);
        for (int i = 0; i < tickCount; i++) {
            features.add(new Feature(min + i * stepSize));
        }
        return new Quantizer(features);
    }
}
