package neptune.geospatial.util.trie;


import org.apache.log4j.Logger;

import java.util.*;

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

    public Node(String prefix, String computationId, String ctrlEndpoint) {
        this.prefix = prefix;
        this.prefixLength = prefix.length() - 1;
        this.computationId = computationId;
        this.ctrlEndpoint = ctrlEndpoint;
    }

    /**
     * Constructor for Root node
     */
    public Node() {
        this.prefix = "_";
        this.prefixLength = 1; // assuming the minimum length we consider is 2 characters
        this.computationId = "";
        this.ctrlEndpoint = "";
    }

    /**
     * Registers a new prefix in an existing node.
     *
     * @param node A node representing the given sub-prefix
     */
    protected void add(Node node) {
        Node longestPrefixMatch = getChildWithLongestMatchingPrefix(node.prefix);
        if (longestPrefixMatch == null) {
            if (isRoot()) {
                childNodes.put(node.prefix, node);
            } else {
                if (this.computationId.equals(node.computationId)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(String.format("[Trie: %s] New prefix %s added to %s", prefix, node.prefix,
                                computationId));
                    }
                } else {
                    logger.error(String.format("[Trie: %s] Error in the trie. Conflicting endpoints for the prefix. " +
                                    "Prefix: %s, Provided Comp: %s, Expected Comp: %s", prefix, node.prefix,
                            node.computationId, computationId));
                }
            }
        } else {
            longestPrefixMatch.add(node);
        }
    }

    private Node getChildWithLongestMatchingPrefix(String prefix) {
        Node node = null;
        int longestMatchLen = 0;
        for (String childPrefix : childNodes.keySet()) {
            if (prefix.contains(childPrefix)) {
                if (childPrefix.length() > longestMatchLen) {
                    longestMatchLen = childPrefix.length();
                    node = childNodes.get(childPrefix);
                }
            }
        }
        return node;
    }

    private boolean isRoot() {
        return prefix.equals("_");
    }

    /**
     * Scales out a given prefix
     *
     * @param node A node representing the given sub-prefix
     */
    protected void expand(Node node) {
        Node longestMatchingPrefix = getChildWithLongestMatchingPrefix(node.prefix);
        if (longestMatchingPrefix == null) {
            childNodes.put(node.prefix, node);
        } else {
            longestMatchingPrefix.expand(node);
        }
    }

    /**
     * Scales in the given sub-prefix.
     *
     * @param node A node representing the given sub-prefix.
     */
    protected void shrink(Node node) {
        Node longestPrefixMatch = getChildWithLongestMatchingPrefix(node.prefix);
        if (longestPrefixMatch == null) {
            logger.warn(String.format("Invalid shrink request. Current prefix: %s, Shrink Request: %s", prefix, node.prefix));
        } else {
            if (longestPrefixMatch.prefix.equals(node.prefix)) {
                childNodes.remove(node.prefix);
            } else {
                longestPrefixMatch.shrink(node);
            }
        }
    }

    /**
     * Traverse the prefix tree in a depth first manner and return the list of prefixes.
     *
     * @return a list of prefixes. Each node returns a sorted list of child prefixes followed by its own prefix
     */
    protected List<String> traverse() {
        // first sort the children
        TreeSet<String> sortedKeys = new TreeSet<>(childNodes.keySet());
        List<String> traverseResults = new ArrayList<>();
        // for each child, call traverse and concatenate their list.
        for (String key : sortedKeys) {
            List<String> childTraverseResults = childNodes.get(key).traverse();
            for (String childTraverseResult : childTraverseResults) {
                childTraverseResult += "(" + this.prefix + ")";
                traverseResults.add(childTraverseResult);
            }
        }
        traverseResults.add(prefix);
        return traverseResults;
    }

    /**
     * Encodes the results of a traverse into a single String
     *
     * @param traverseResults List of prefixes returned by the {@code traverse} method.
     * @return A string encoded version of the prefix results.
     */
    protected String printTraverseResults(List<String> traverseResults) {
        StringBuilder sBuilder = new StringBuilder();
        for (String prefix : traverseResults) {
            if (prefix.equals("")) {
                sBuilder.append("_");
            } else {
                sBuilder.append(prefix);
            }
            sBuilder.append(":");
        }
        sBuilder.deleteCharAt(sBuilder.lastIndexOf(":"));
        return sBuilder.toString();
    }

    public static void main(String[] args) {
        Node root = new Node();
        System.out.println(root.printTraverseResults(root.traverse()));
        root.add(new Node("9XA", "comp_id-1", "localhost:9099"));
        root.add(new Node("9JB", "comp_id-1", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));
        root.add(new Node("8GF", "comp_id-2", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));
        root.add(new Node("ABC", "comp_id-3", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));

        System.out.println("\nExpanding ----------------------");
        root.expand(new Node("9X1", "comp_id-4", "localhost:9099"));
        root.expand(new Node("9X2", "comp_id-5", "localhost:9099"));
        root.expand(new Node("9X1A", "comp_id-6", "localhost:9099"));
        root.expand(new Node("9X3", "comp_id-7", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));

        System.out.println("\nShrinking 9X1 ----------------------");
        root.shrink(new Node("9X1", "comp_id-1", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));

        System.out.println("\nAdding 9X1 ----------------------");
        root.add(new Node("9X1", "comp_id-1", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));

        System.out.println("\nExpanding 9X1 ----------------------");
        root.expand(new Node("9X1", "comp_id-4", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));

        System.out.println("\nAdding 9X1A2 ----------------------");
        root.add(new Node("9X1A2", "comp_id-4", "localhost:9099"));
        System.out.println(root.printTraverseResults(root.traverse()));
    }

}
