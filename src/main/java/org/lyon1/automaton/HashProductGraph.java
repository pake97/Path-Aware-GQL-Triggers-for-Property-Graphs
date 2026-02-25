package org.lyon1.automaton;

import java.util.*;

public class HashProductGraph implements ProductGraphInterface {

    // (elementId + ":" + state) -> product node
    private final Map<String, ProductNode> index = new HashMap<>();

    // product node -> successors
    private final Map<ProductNode, Set<ProductNode>> adjacency = new HashMap<>();

    // product node -> predecessors
    private final Map<ProductNode, Set<ProductNode>> reverseAdjacency = new HashMap<>();

    public ProductNode addNode(String elementId, String state, StateType type, long timestamp) {
        ProductNode node = new ProductNode(elementId, state, type, timestamp);
        index.put(elementId + ":" + state, node);
        adjacency.putIfAbsent(node, new HashSet<>());
        reverseAdjacency.putIfAbsent(node, new HashSet<>());
        return node;
    }

    @Override
    public ProductNode getOrCreate(String elementId, String state, StateType type, long timestamp) {
        String key = elementId + ":" + state;
        ProductNode node = index.get(key);
        if (node == null) {
            node = new ProductNode(elementId, state, type, timestamp);
            index.put(key, node);
            adjacency.putIfAbsent(node, new HashSet<>());
            reverseAdjacency.putIfAbsent(node, new HashSet<>());
        }
        return node;
    }

    @Override
    public ProductNode get(String elementId) {
        // This method is now ambiguous since an element can have multiple states.
        // For simplicity, we return the first one found or null.
        // In this specific refactoring, we might want to get all nodes for an element
        // ID.
        return index.values().stream()
                .filter(n -> n.elementId().equals(elementId))
                .findFirst()
                .orElse(null);
    }

    public List<ProductNode> getAllNodes(String elementId) {
        return index.values().stream()
                .filter(n -> n.elementId().equals(elementId))
                .toList();
    }

    public ProductNode get(String elementId, String state) {
        return index.get(elementId + ":" + state);
    }

    @Override
    public void addEdge(ProductNode from, ProductNode to) {
        adjacency
                .computeIfAbsent(from, k -> new HashSet<>())
                .add(to);
        reverseAdjacency
                .computeIfAbsent(to, k -> new HashSet<>())
                .add(from);
    }

    @Override
    public Collection<ProductNode> successors(ProductNode node) {
        Set<ProductNode> succ = adjacency.get(node);
        return (succ == null) ? Collections.emptyList() : Collections.unmodifiableCollection(succ);
    }

    public Collection<ProductNode> predecessors(ProductNode node) {
        Set<ProductNode> pred = reverseAdjacency.get(node);
        return (pred == null) ? Collections.emptyList() : Collections.unmodifiableCollection(pred);
    }
}
