package synopsis.external.util;

import neptune.geospatial.util.trie.GeoHashPrefixTree;
import synopsis.client.persistence.OutstandingPersistenceTask;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class PrefixFreqCounter {
    public static void main(String[] args) {
        String pathToFreqFiles = "/Users/thilina/Desktop/freq_files";
        File dir = new File(pathToFreqFiles);
        if (!(dir.exists() && dir.isDirectory())) {
            System.err.println("Invalid input path: " + pathToFreqFiles);
        }
        File[] freqfiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".fstat");
            }
        });
        HashMap<String, Long> prefixFreqMap = new HashMap<>();
        int counter = freqfiles.length;
        for (File freqFile : freqfiles) {
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(new FileReader(freqFile));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] splits = line.split(",");
                    String prefix = splits[0];
                    Long freq = Long.parseLong(splits[1]);
                    if (prefixFreqMap.containsKey(prefix)) {
                        freq += prefixFreqMap.get(prefix);
                    }
                    prefixFreqMap.put(prefix, freq);
                }
                counter--;
                System.out.println("Processed a freq. file. Remaining count: " + counter + ", " +
                        "Freq. Map Size: " + prefixFreqMap.size());
            } catch (IOException e) {
                System.err.println("Error processing freq. file.");
                e.printStackTrace();
                return;
            } finally {
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) {
                        System.err.println("Error closing file reader.");
                        e.printStackTrace();
                        return;
                    }
                }
            }
        }

        Map<String, Long> shrinked = new HashMap<>();
        for(String p : prefixFreqMap.keySet()){
            String shrinkedPrefix = p.substring(0, 3);
            long count = prefixFreqMap.get(p);
            if(shrinked.containsKey(shrinkedPrefix)){
                count += shrinked.get(shrinkedPrefix);
            }
            shrinked.put(shrinkedPrefix, count);
        }

        // read and populate the prefix tree
        String prefixTreePath = "/Users/thilina/Desktop/2014_6f_full.pstat";
        OutstandingPersistenceTask task = Util.deserializeOutstandingPersistenceTask(prefixTreePath);
        if (task == null) {
            System.err.println("Error deserializing the task.");
            return;
        }
        byte[] serializedPrefixTree = task.getSerializedPrefixTree();
        GeoHashPrefixTree prefixTree = GeoHashPrefixTree.getInstance();
        try {
            prefixTree.deserialize(serializedPrefixTree);
        } catch (IOException e) {
            System.err.println("Error deserializing the prefix tree.");
            e.printStackTrace();
            return;
        }
        try {
            FileWriter fileW = new FileWriter("/tmp/freq-count-depth.csv");
            BufferedWriter bufferedWriter = new BufferedWriter(fileW);
            for (String prefix : shrinked.keySet()) {
                int sketchletCount = prefixTree.query(prefix).size();
                int depth = prefixTree.getDepth(prefix);
                long count = shrinked.get(prefix);
                bufferedWriter.write(prefix + "," + count + "," + sketchletCount + "," + depth + "\n");
            }
            bufferedWriter.flush();
            fileW.flush();
            bufferedWriter.close();
            fileW.close();
            System.out.println("Completed writing frequency-depth counts to file.");
        } catch (IOException e) {
            System.err.println("Error writing freq-depth counts into a file.");
            e.printStackTrace();
        }
    }
}
