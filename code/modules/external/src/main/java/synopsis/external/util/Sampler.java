
package synopsis.external.util;

import java.io.*;
import java.util.Random;

/**
 * @author Thilina Buddhika
 */
public class Sampler {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Not enough arguments. Usage: input_dir sampling_percentage(e.g.: 0.10)");
            System.exit(-1);
        }
        String inputDir = args[0];
        double samplePercentage = Double.parseDouble(args[1]);
        System.out.println("Using input directory: " + inputDir);
        System.out.println("Sampling percentage: " + samplePercentage * 100 + "%");

        File[] inputFiles = new File(inputDir).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("southeast-0") && name.endsWith(".txt");
            }
        });
        Random random = new Random();
        long totalRead = 0;
        long totalWritten = 0;
        int completedFileCount = 0;
        System.out.println("Input file count: " + inputFiles.length);
        FileWriter fileWriter = null;
        BufferedWriter bfw = null;
        try {
            fileWriter = new FileWriter(inputDir + File.separator + "sample-" + samplePercentage * 100 + ".txt");
            bfw = new BufferedWriter(fileWriter);
            for (File inputF : inputFiles) {
                FileReader fr = new FileReader(inputF);
                BufferedReader bfr = new BufferedReader(fr);
                String line;
                while ((line = bfr.readLine()) != null) {
                    if (random.nextDouble() <= samplePercentage) {
                        bfw.write(line);
                        bfw.newLine();
                        totalWritten++;
                    }
                    totalRead++;
                }
                bfr.close();
                fr.close();
                bfw.flush();
                fileWriter.flush();
                System.out.println("Completed " + (++completedFileCount) + " of " + inputFiles.length + " files.");
                System.out.println("Total Read: " + totalRead + ", Total Written: " + totalWritten + ", Percentage: " + ((totalWritten * 1.0)/totalRead * 100));
            }
        } catch (IOException e) {
            System.err.println("Error reading/writing to sample file.");
            e.printStackTrace();
        } finally {
            try {
                if(bfw != null) {
                    bfw.flush();
                }
                if(fileWriter != null){
                    fileWriter.flush();
                }
                if(bfw != null){
                    bfw.close();
                }
                if(fileWriter != null){
                    fileWriter.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing file streams.");
                e.printStackTrace();
            }
        }
        System.out.println("Completed!");
    }
}
