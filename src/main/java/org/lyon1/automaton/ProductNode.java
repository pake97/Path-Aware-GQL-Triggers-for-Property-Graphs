package org.lyon1.automaton;
enum StateType { NORMAL, ACCEPTING, INITIAL }

public record ProductNode(String State, StateType type, long timestamp) {}

