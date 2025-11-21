package org.lyon1.automaton;

import java.util.Collection;

public interface ProductGraphInterface {

    /**
     * Get an existing product node or create it if missing.
     */
    ProductNode getOrCreate(String elementId, String state, StateType type, long timestamp);

    /**
     * Get an existing product node, or null if it doesn’t exist.
     */
    ProductNode get(String elementId);


    /**
     * Add a directed product edge: from -> to.
     */
    void addEdge(ProductNode from, ProductNode to);

    /**
     * Successors in the product graph.
     */
    Collection<ProductNode> successors(ProductNode node);
}
