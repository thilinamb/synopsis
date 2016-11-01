package synopsis.external.util;

import neptune.geospatial.util.trie.GeoHashPrefixTree;
import neptune.geospatial.util.trie.Node;
import synopsis.client.persistence.OutstandingPersistenceTask;

import java.io.*;
import java.util.*;

/**
 * @author Thilina Buddhika
 */
public class AdjacencyListSerializer {

    private class Edge implements Comparable<Edge>{
        private int from;
        private int to;

        public Edge(int from, int to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public int compareTo(Edge o) {
            if (o.from == this.from){
                return new Integer(this.to).compareTo(o.to);
            } else {
                return new Integer(this.from).compareTo(o.from);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Edge edge = (Edge) o;

            if (from != edge.from) return false;
            return to == edge.to;

        }

        @Override
        public int hashCode() {
            int result = from;
            result = 31 * result + to;
            return result;
        }
    }

    private GeoHashPrefixTree prefixTree;
    private HashMap<String, Integer> numericIds = new HashMap<>();
    private int lastIssuedId = 0;

    public AdjacencyListSerializer(GeoHashPrefixTree prefixTree) {
        this.prefixTree = prefixTree;
    }

    public void serializeAsAdjacencyList(Writer dos) throws IOException {
        Node root = prefixTree.getRoot();
        List<Edge> edges = serializeNode(root);
        System.out.println("total number of nodes: " + lastIssuedId);
        Collections.sort(edges);
        List<Edge> seen = new ArrayList<>();
        for(Edge edge: edges){
            if(!seen.contains(edge)) {
                dos.write(edge.from + "," + edge.to + "\n");
                seen.add(edge);
            }
        }
    }

    private int getNumericIdentifierForVertex(String vertex) {
        int id;
        if (numericIds.containsKey(vertex)) {
            id = numericIds.get(vertex);
        } else {
            id = lastIssuedId++;
            numericIds.put(vertex, id);
        }
        return id;
    }

    private List<Edge> serializeNode(Node node) throws IOException {
        Set<String> childDestinationNodes = new HashSet<>();
        for (Node child : node.getChildNodes().values()) {
            childDestinationNodes.add(child.getCtrlEndpoint());
        }
        String compId = node.isRoot() ? "root" : node.getCtrlEndpoint();
        int id = getNumericIdentifierForVertex(compId);
        List<Edge> edges = new ArrayList<>();
        for (String dest : childDestinationNodes) {
            int childId = getNumericIdentifierForVertex(dest);
            edges.add(new Edge(id, childId));
        }
        for (Node child : node.getChildNodes().values()) {
            edges.addAll(serializeNode(child));
        }
        return edges;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: <location_of_the_serialized_deployment_plan>");
        }
        String serializedDeplomentPlanLoc = args[0];
        OutstandingPersistenceTask outstandingPersistenceTask = Util.deserializeOutstandingPersistenceTask(serializedDeplomentPlanLoc);

        if (outstandingPersistenceTask == null) {
            System.err.println("OutstandingPersistenceTask is null. Exiting!");
            return;
        }

        byte[] serializedPrefixTree = outstandingPersistenceTask.getSerializedPrefixTree();
        GeoHashPrefixTree prefixTree = GeoHashPrefixTree.getInstance();
        try {
            prefixTree.deserialize(serializedPrefixTree);
            System.out.println("Successfully reconstructed the prefix tree!");
            AdjacencyListSerializer adjacencyListSerializer = new AdjacencyListSerializer(prefixTree);
            BufferedWriter buffW = new BufferedWriter(new FileWriter("/tmp/adjaceny-list.txt"));
            adjacencyListSerializer.serializeAsAdjacencyList(buffW);
            buffW.flush();
            buffW.close();
            System.out.println("Successfully stored adjacency list!");
        } catch (IOException e) {
            System.out.println("Error deserializing the prefix tree.");
        }
    }
}
