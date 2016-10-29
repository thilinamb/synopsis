package synopsis.external.util;

import neptune.geospatial.util.trie.GeoHashPrefixTree;
import neptune.geospatial.util.trie.Node;
import synopsis.client.persistence.OutstandingPersistenceTask;

import java.io.*;

/**
 * @author Thilina Buddhika
 */
public class AdjacencyListSerializer {

    private GeoHashPrefixTree prefixTree;

    public AdjacencyListSerializer(GeoHashPrefixTree prefixTree) {
        this.prefixTree = prefixTree;
    }

    public void serializeAsAdjacencyList(DataOutputStream dos) throws IOException {
        Node root = prefixTree.getRoot();
        serializeNode(root, dos);
    }

    private void serializeNode(Node node, DataOutputStream dos) throws IOException {
        dos.writeUTF(node.getPrefix() + " " + node.getChildNodes().size() + "\n");
        for (Node child : node.getChildNodes().values()) {
            dos.writeUTF(child.getPrefix() + "\n");
        }
        for (Node child : node.getChildNodes().values()) {
            serializeNode(child, dos);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: <location_of_the_serialized_deployment_plan>");
        }
        String serializedDeplomentPlanLoc = args[0];
        FileInputStream fis = null;
        DataInputStream dis = null;
        OutstandingPersistenceTask outstandingPersistenceTask = null;
        try {
            fis = new FileInputStream(serializedDeplomentPlanLoc);
            dis = new DataInputStream(fis);
            outstandingPersistenceTask = new OutstandingPersistenceTask();
            outstandingPersistenceTask.deserialize(dis);
            System.out.println("Successfully deserliazed the OutstandingPersistenceTask!");
        } catch (IOException e) {
            System.err.println("Error reading the deployment plan.");
            e.printStackTrace();
            System.exit(-1);
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
                if (dis != null) {
                    dis.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing file streams.");
                e.printStackTrace();
            }
        }

        if (outstandingPersistenceTask == null) {
            System.err.println("OutstandingPersistenceTask is null. Exiting!");
            return;
        }

        byte[] serializedPrefixTree = outstandingPersistenceTask.getSerializedPrefixTree();
        GeoHashPrefixTree prefixTree = GeoHashPrefixTree.getInstance();
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            prefixTree.deserialize(serializedPrefixTree);
            System.out.println("Successfully reconstructed the prefix tree!");
            AdjacencyListSerializer adjacencyListSerializer = new AdjacencyListSerializer(prefixTree);
            fos = new FileOutputStream("/tmp/adjaceny-list.txt");
            dos = new DataOutputStream(fos);
            adjacencyListSerializer.serializeAsAdjacencyList(dos);
            dos.flush();
            fos.flush();
            System.out.println("Successfully stored adjacency list!");
        } catch (IOException e) {
            System.out.println("Error deserializing the prefix tree.");
        } finally {
            try {
                if(fos != null){
                    fos.close();
                }
                if(dos != null){
                    dos.close();
                }
            } catch (IOException e) {
                System.err.println("Error clsoing file streams.");
                e.printStackTrace();
            }
        }
    }
}
