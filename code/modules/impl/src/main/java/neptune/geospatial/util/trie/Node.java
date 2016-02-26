package neptune.geospatial.util.trie;


import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class Node {

    private Logger logger = Logger.getLogger(Node.class);

    /**
     * The number of bits/chars the prefixes are advanced when traversing the tree.
     */
    public static final int CHILD_NODE_QUALIFIER_LENGTH = 1;

    /**
     * prefix length.
     */
    private int prefixLength;

    /**
     * prefix it is responsible for
     */
    private String prefix;

    /**
     * Child nodes
     */
    private Map<String, Node> childNodes = new HashMap<>();

    /**
     * Computation which is responsible for the prefixes
     */
    private String computationId;

    /**
     * Control endpoint of the Granules resource where the computation is located
     */
    private String ctrlEndpoint;

    public Node(String prefix) {
        this.prefix = prefix;
        this.prefixLength = prefix.length();
    }

    // void insert(Node node, String prefix), Node search(string prefix)
    public void add(Node node) {
        String newNodePrefix = node.prefix;
        String childQualifier = newNodePrefix.substring(prefixLength, prefixLength + CHILD_NODE_QUALIFIER_LENGTH);
        if (childNodes.containsKey(childQualifier)) { // there is a child node who can handles a longer prefix.
            childNodes.get(childQualifier).add(node);
        } else { // it should be a prefix that the current node maintains.
            if (this.computationId.equals(node.computationId) && (this.ctrlEndpoint.equals(node.ctrlEndpoint))) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("[Trie: %s] New prefix %s added to %s", prefix, newNodePrefix,
                            computationId));
                } else {
                    logger.error(String.format("[Trie: %s] Error in the trie. Conflicting endpoints for the prefix. " +
                                    "Prefix: %s, Provided Comp: %s, Expected Comp: %s", prefix, newNodePrefix,
                            node.computationId, computationId));
                }
            }
        }
    }

    public void expand(Node node) {
        String newNodePrefix = node.prefix;
        String childQualifier = newNodePrefix.substring(prefixLength, prefixLength + CHILD_NODE_QUALIFIER_LENGTH);
        if (childNodes.containsKey(childQualifier)) { // there is a child node who can handles a longer prefix.
            childNodes.get(childQualifier).expand(node);
        } else {
            childNodes.put(childQualifier, node);
            if(logger.isDebugEnabled()){
                logger.debug(String.format("[Trie - %s] Trie is expanded. A new child node added. New prefix: %s, " +
                        "New Comp: %s", prefix, newNodePrefix, node.computationId));
            }
        }
    }

    public void shrink(Node node) {
        String newNodePrefix = node.prefix;
        String childQualifier = newNodePrefix.substring(prefixLength, prefixLength + CHILD_NODE_QUALIFIER_LENGTH);
        if (childNodes.containsKey(childQualifier)) { // there is a child node who can handles a longer prefix.
            if (childNodes.get(childQualifier).prefixLength == newNodePrefix.length()) {
                childNodes.remove(childQualifier);
            } else {
                childNodes.get(childQualifier).shrink(node);
            }
        } else {
            logger.error(String.format("[Trie - %s] Error in the trie. Trying to shrink non-existing child." +
                    " Child Prefix: %s", prefix, newNodePrefix));
        }
    }

    public static void main(String[] args) {

    }

}
