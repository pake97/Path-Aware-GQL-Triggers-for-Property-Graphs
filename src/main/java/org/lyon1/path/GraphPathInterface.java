package org.lyon1.path;

import java.util.List;

/**
 * Represents a Cypher-style graph path:
 * (n0)-[r0]->(n1)-[r1]-> ... ->(n_k)
 *
 * Nodes and relationships are generic so you can plug in your own
 * Node / Relationship types (e.g. Neo4j's org.neo4j.graphdb.Node).
 */
public interface GraphPathInterface<N, R> {

    /**
     * @return ordered list of nodes in the path (never null, size >= 1)
     */
    List<N> getNodes();

    /**
     * @return ordered list of relationships in the path
     *         (never null, size = getNodes().size() - 1, or 0 for a single node)
     */
    List<R> getRelationships();

    /**
     * @return the start node (first node in the path)
     */
    default N getStart() {
        return getNodes().get(0);
    }

    /**
     * @return the end node (last node in the path)
     */
    default N getEnd() {
        List<N> nodes = getNodes();
        return nodes.get(nodes.size() - 1);
    }

    /**
     * Cypher path length = number of relationships.
     */
    default int length() {
        return getRelationships().size();
    }
}
