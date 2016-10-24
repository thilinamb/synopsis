package neptune.geospatial.util.trie;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import neptune.geospatial.hazelcast.type.SketchLocation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A customized version of a prefix tree representing the
 * distributed sketch.
 * This is an eventually consistent data structure.
 *
 * @author Thilina Buddhika
 */
public class GeoHashPrefixTree implements EntryAddedListener<String, SketchLocation>,
        EntryUpdatedListener<String, SketchLocation> {

    public static final String PREFIX_MAP = "prefix-map";
    private static final GeoHashPrefixTree instance = new GeoHashPrefixTree();
    /**
     * root node is a synthetic node which doesn't exist
     * in the real distributed setup.
     */
    private Node root;

    private GeoHashPrefixTree() {
        this.root = new Node();
    }

    public static GeoHashPrefixTree getInstance(){
        return instance;
    }

    /**
     * Register a new prefix. Does not perform and expansion or shrink of the prefix tree.
     *
     * @param prefix       Prefix
     * @param computation  Computation Id handling the prefix
     * @param ctrlEndpoint Control Endpoint hosting the computation
     */
    private synchronized void registerPrefix(String prefix, String computation, String ctrlEndpoint) {
        Node node = new Node(prefix.substring(0, prefix.length() - 1), computation, ctrlEndpoint);
        root.add(node);
    }

    /**
     * Expands the prefix tree for a given prefix
     *
     * @param prefix    Prefix that is being scaled out
     * @param newCompId New computation
     * @param newCtrlEp Location of the new computation
     */
    private synchronized void recordScaleOut(String prefix, String newCompId, String newCtrlEp) {
        Node node = new Node(prefix, newCompId, newCtrlEp);
        root.expand(node);
    }

    /**
     * Shrinks the prefix tree for a given prefix
     *
     * @param prefix Prefix which is scaled in
     * @param compId Computation which is going to hold the prefix after scaling in
     * @param ctrlEp location of the above computation
     */
    private synchronized void recordScaleIn(String prefix, String compId, String ctrlEp) {
        Node node = new Node(prefix, compId, ctrlEp);
        root.shrink(node);
    }

    /**
     * Query the trie and return the list of computations and their endpoints where the
     * sketchlets corresponding to prefixes are stored
     * @param prefix Prefix string to be queried
     * @return Map of ComputationId to ctrl_endpoint
     */
    public synchronized Map<String, String> query(String prefix) {
        Map<String, String> locationMap = new HashMap<>();
        List<Node> nodes = root.query(prefix);
        for (Node n : nodes) {
            locationMap.put(n.getComputationId(), n.getCtrlEndpoint());
        }
        return locationMap;
    }

    @Override
    public void entryAdded(EntryEvent<String, SketchLocation> entryEvent) {
        String prefix = entryEvent.getKey();
        SketchLocation sketchLocation = entryEvent.getValue();
        if (sketchLocation.getMode() == SketchLocation.MODE_REGISTER_NEW_PREFIX) {
            registerPrefix(prefix, sketchLocation.getComputation(), sketchLocation.getCtrlEndpoint());
        }
    }

    @Override
    public void entryUpdated(EntryEvent<String, SketchLocation> entryEvent) {
        String prefix = entryEvent.getKey();
        SketchLocation sketchLocation = entryEvent.getValue();
        if (sketchLocation.getMode() == SketchLocation.MODE_SCALE_IN) {
            recordScaleIn(prefix, sketchLocation.getComputation(), sketchLocation.getCtrlEndpoint());
        } else if (sketchLocation.getMode() == SketchLocation.MODE_SCALE_OUT) {
            recordScaleOut(prefix, sketchLocation.getComputation(), sketchLocation.getCtrlEndpoint());
        }
    }

    public byte[] serialize() throws IOException {
        return root.serialize();
    }

    public void deserialize(byte[] bytes) throws IOException{
        root.deserialize(bytes);
    }

    public Node getRoot() {
        return root;
    }

    private String printTree() {
        return root.printTraverseResults(root.traverse());
    }

    public static void main(String[] args) {
        GeoHashPrefixTree prefixTree = new GeoHashPrefixTree();
        String[] computations = {"comp-1", "comp-2", "comp-3"};
        String[] endpoints = {"endpoint-1", "endpoint-2", "endpoint-3"};
        prefixTree.registerPrefix("8GF", computations[0], endpoints[0]);
        //System.out.println(prefixTree.printTree());
        prefixTree.registerPrefix("9X1", computations[1], endpoints[1]);
        //System.out.println(prefixTree.printTree());
        prefixTree.registerPrefix("9X2", computations[1], endpoints[1]);
        //System.out.println(prefixTree.printTree());
        prefixTree.registerPrefix("9X1A", computations[2], endpoints[2]);
        //System.out.println(prefixTree.printTree());
        prefixTree.recordScaleOut("9X1", computations[2], endpoints[2]);
        //System.out.println(prefixTree.printTree());
        prefixTree.recordScaleOut("9X1A", computations[1], endpoints[1]);
        System.out.println(prefixTree.printTree());

        /*
        System.out.println("------------ Querying -------------------");
        // querying
        printNodeList(prefixTree.query("9X1A"));
        System.out.println("");
        printNodeList(prefixTree.query("9X1"));
        System.out.println("");
        printNodeList(prefixTree.query("9X"));
        // empty results
        System.out.println("");
        printNodeList(prefixTree.query("A1"));
        // unseen child prefix
        System.out.println("");
        printNodeList(prefixTree.query("9XB"));
        */

        // serialization test
        try {
            byte[] serializedData = prefixTree.serialize();
            GeoHashPrefixTree prefTree2 = new GeoHashPrefixTree();
            System.out.println("empty tree: " + prefTree2.printTree());
            prefTree2.deserialize(serializedData);
            System.out.println("After populating: " + prefTree2.printTree());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printNodeList(Map<String, String> locs) {
        if(locs.size() > 0) {
            for (String compId : locs.keySet()) {
                System.out.println(compId + "->" + locs.get(compId));
            }
        } else {
            System.out.println("Empty map!");
        }
    }
}
