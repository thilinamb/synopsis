package neptune.geospatial.util.trie;


import ds.granules.exception.GranulesConfigurationException;
import neptune.geospatial.util.RivuletUtil;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * Implements a customized version of a prefix tree.
 * The prefixes are not incremented one character at a time, instead takes leaps
 * of multiple characters at a time.
 *
 * @author Thilina Buddhika
 */
class Node {

    private Logger logger = Logger.getLogger(Node.class);

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

    Node(String prefix, String computationId, String ctrlEndpoint) {
        this.prefix = prefix;
        this.computationId = computationId;
        this.ctrlEndpoint = ctrlEndpoint;
    }

    /**
     * Constructor for Root node
     */
    Node() {
        this.prefix = "_";
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
                    if (logger.isTraceEnabled() || shouldLogToConsole()) {
                        // todo: info -> trace
                        logger.info(String.format("[%s] New prefix %s added to %s", prefix, node.prefix,
                                computationId));
                    }
                } else {
                    if (logger.isTraceEnabled() || shouldLogToConsole()) {
                        // todo: info -> trace
                        logger.info(String.format("[%s] Conflicting endpoints for the prefix. " +
                                        "Prefix: %s, Provided Comp: %s, Expected Comp: %s", prefix, node.prefix,
                                node.computationId, computationId));
                    }
                }
            }
        } else {
            longestPrefixMatch.add(node);
        }
    }

    /**
     * Scales out a given prefix
     *
     * @param node A node representing the given sub-prefix
     */
    void expand(Node node) {
        Node longestMatchingPrefix = getChildWithLongestMatchingPrefix(node.prefix);
        if (longestMatchingPrefix == null) {
            childNodes.put(node.prefix, node);
            if (logger.isTraceEnabled() || shouldLogToConsole()) {
                // todo: info -> trace
                logger.info(String.format("[%s] Prefix tree is expanded. Child Prefix: %s", this.prefix, node.prefix));
            }
        } else {
            longestMatchingPrefix.expand(node);
        }
    }

    /**
     * Scales in the given sub-prefix.
     *
     * @param node A node representing the given sub-prefix.
     */
    void shrink(Node node) {
        Node longestPrefixMatch = getChildWithLongestMatchingPrefix(node.prefix);
        if (longestPrefixMatch == null) {
            logger.error(String.format("Invalid shrink request. Current prefix: %s, Shrink Request: %s", prefix, node.prefix));
        } else {
            if (longestPrefixMatch.prefix.equals(node.prefix)) {
                childNodes.remove(node.prefix);
                if (logger.isTraceEnabled() || shouldLogToConsole()) {
                    // todo: info -> trace
                    logger.info(String.format("[%s] Prefix tree is shrinked. Child Prefix: %s", this.prefix, node.prefix));
                }
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
    List<String> traverse() {
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
    String printTraverseResults(List<String> traverseResults) {
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

    // TODO: remove this
    // this is a temporary method added to make sure that the logs are printed in a single node
    private boolean shouldLogToConsole() {
        try {
            return RivuletUtil.getCtrlEndpoint().startsWith("lattice-35");
        } catch (GranulesConfigurationException e) {
            return false;
        }
    }

    /**
     * Query the subtree where the current node is the root for the given prefix
     *
     * @param prefix Given prefix for querying
     * @return List of nodes corresponding to the prefix (comprised of child nodes and current node)
     */
    public List<Node> query(String prefix) {
        if (this.prefix.equals(prefix)) {
            return getChildrenAsFlatList();
        }
        Node longestMatchingChild = getChildWithLongestMatchingPrefix(prefix);
        if (longestMatchingChild != null) {
            return longestMatchingChild.query(prefix);
        } else {
            List<Node> nodes = new ArrayList<>();
            if (!this.isRoot()) {
                nodes.add(this);
            }
            return nodes;
        }
    }

    /**
     * A list of all children traversing the trie in depth first
     *
     * @return A flat list of child nodes - no duplicate elimination
     */
    private List<Node> getChildrenAsFlatList() {
        List<Node> flatList = new ArrayList<>();
        for (Node child : childNodes.values()) {
            flatList.addAll(child.getChildrenAsFlatList());
        }
        flatList.add(this);
        return flatList;
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = null;
        DataOutputStream dos = null;
        try {
            baos = new ByteArrayOutputStream();
            dos = new DataOutputStream(baos);
            dos.writeUTF(this.prefix);
            dos.writeUTF(this.computationId);
            dos.writeUTF(this.ctrlEndpoint);
            dos.writeInt(this.childNodes.size());
            for (String childPref : this.childNodes.keySet()) {
                dos.writeUTF(childPref);
                byte[] serializedChildNode = this.childNodes.get(childPref).serialize();
                dos.writeInt(serializedChildNode.length);
                dos.write(this.childNodes.get(childPref).serialize());
            }
            dos.flush();
            return baos.toByteArray();
        } finally {
            if (dos != null) {
                dos.close();
            }
            if (baos != null) {
                baos.close();
            }
        }
    }

    public void deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = null;
        DataInputStream dis = null;
        try {
            bais = new ByteArrayInputStream(bytes);
            dis = new DataInputStream(bais);
            this.prefix = dis.readUTF();
            this.computationId = dis.readUTF();
            this.ctrlEndpoint = dis.readUTF();
            int childNodeCount = dis.readInt();
            for (int i = 0; i < childNodeCount; i++) {
                String childPrefix = dis.readUTF();
                int serializedDataLength = dis.readInt();
                byte[] serializedData = new byte[serializedDataLength];
                dis.readFully(serializedData);
                Node childNode = new Node();
                childNode.deserialize(serializedData);
                this.childNodes.put(childPrefix, childNode);
            }
        } finally {
            if (dis != null) {
                dis.close();
            }
            if (bais != null) {
                bais.close();
            }
        }
    }

    @Override
    public String toString() {
        return "Node{" +
                "prefix='" + prefix + '\'' +
                ", computationId='" + computationId + '\'' +
                ", ctrlEndpoint='" + ctrlEndpoint + '\'' +
                '}';
    }

    public String getComputationId() {
        return computationId;
    }

    public String getCtrlEndpoint() {
        return ctrlEndpoint;
    }
}
