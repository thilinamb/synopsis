package neptune.geospatial.util.trie;

/**
 * A customized version of a prefix tree representing the
 * distributed sketch.
 *
 * @author Thilina Buddhika
 */
public class GeoHashPrefixTree {

    /**
     * root node is a synthetic node which doesn't exist
     * in the real distributed setup.
     */
    private Node root = new Node("", "", "");

    /**
     * Register a new prefix. Does not perform and expansion or shrink of the prefix tree.
     * @param prefix Prefix
     * @param computation Computation Id handling the prefix
     * @param ctrlEndpoint Control Endpoint hosting the computation
     */
    public void registerPrefix(String prefix, String computation, String ctrlEndpoint) {
        Node node = new Node(prefix, computation, ctrlEndpoint);
        root.add(node);
    }

    /**
     * Expands the prefix tree for a given prefix
     * @param prefix Prefix that is being scaled out
     * @param newCompId New computation
     * @param newCtrlEp Location of the new computation
     */
    public void recordScaleOut(String prefix, String newCompId, String newCtrlEp){
        Node node = new Node(prefix, newCompId, newCtrlEp);
        root.expand(node);
    }

    /**
     * Shrinks the prefix tree for a given prefix
     * @param prefix Prefix which is scaled in
     * @param compId Computation which is going to hold the prefix after scaling in
     * @param ctrlEp location of the above computation
     */
    public void recordScaleIn(String prefix, String compId, String ctrlEp){
        Node node = new Node(prefix, compId, ctrlEp);
        root.shrink(node);
    }
}
