package org.lyon1.automaton;

import java.util.*;
import java.util.stream.Collectors;

public class HashProductGraph implements ProductGraphInterface {

    // elementId -> (state -> product node)
    private final Map<String, ProductNode> index = new HashMap<>();

    // product node -> successors
    private final Map<ProductNode, Set<ProductNode>> adjacency = new HashMap<>();


    public ProductNode addNode(String elementId, String state, StateType type, long timestamp) {
        ProductNode node = new ProductNode(state, type, timestamp);
        index.put(elementId, node);
        adjacency.putIfAbsent(node, new HashSet<>());
        return node;
    }


    @Override
    public ProductNode getOrCreate(String elementId, String state, StateType type, long timestamp) {
        ProductNode node = index.get(elementId);
        if(node==null){
            node = new ProductNode(state, type, timestamp);
            index.put(elementId, node);
            adjacency.putIfAbsent(node, new HashSet<>());
        }
        return node;

    }

    @Override
    public ProductNode get(String elementId) {
        return index.get(elementId);

    }

    @Override
    public void addEdge(ProductNode from, ProductNode to) {
        adjacency
                .computeIfAbsent(from, k -> new HashSet<>())
                .add(to);
    }

    @Override
    public Collection<ProductNode> successors(ProductNode node) {
        Set<ProductNode> succ = adjacency.get(node);
        if (succ == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableCollection(succ);
    }
}

