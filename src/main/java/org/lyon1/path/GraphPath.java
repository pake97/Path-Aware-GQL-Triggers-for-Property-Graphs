package org.lyon1.path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable implementation of GraphPath.
 */
public final class GraphPath implements GraphPathInterface {

    private final List<GraphElement> elements;

    public GraphPath(List<GraphElement> elements) {


        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    @Override
    public List<GraphElement> getNodes() {
        return elements.stream().filter(x -> x.isNode()).toList();
    }

    @Override
    public List<GraphElement> getRelationships() {
        return elements.stream().filter(x -> x.isRelationship()).toList();
    }


    public List<GraphElement> getElements(){
        return this.elements;
    }

    public GraphElement get(int index) {
        return elements.get(index);
    }

    @Override
    public String toString() {
        // Simple Cypher-ish rendering using toString() on N and R
        StringBuilder sb = new StringBuilder();
        for(GraphElement e : elements) {
            if(e.isNode()){
                sb.append("(").append(e.getLabel()).append(")");
            }
            else {
                sb.append("-[").append(e.getLabel()).append("]->");
            }
        }

        return sb.toString();
    }
}

// Example usage:
//Building apath:(n1)-[r1:KNOWS]->(n2)
//        *
//GraphPath<MyNode, MyRel> path = new DefaultGraphPath<>(
//        * List.of(new MyNode(1), new MyNode(2)),
// *List.
//
//of(new MyRel(1, "KNOWS"))
//        *);
//        *
//        *System.out.
//
//println(path);          // (n1)-[r1:KNOWS]->(n2)
// *System.out.
//
//println(path.length());
