package org.lyon1.automaton;


public record GraphElementRef(long id, ElementKind kind) {
    public enum ElementKind { NODE, RELATIONSHIP }
}

