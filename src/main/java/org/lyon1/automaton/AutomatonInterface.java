package org.lyon1.automaton;

import java.util.Set;

public interface AutomatonInterface {

    /**
     * Transition structure for this automaton.
     */
    AutomatonTransitionTable getTransitionTable();

    /**
     * States where a run may start.
     * (For DFA, this is a singleton set.)
     */
    Set<String> getInitialStates();

    /**
     * All accepting / final states.
     */
    Set<String> getAcceptingStates();

    /**
     * Is a specific state accepting?
     */
    boolean isAccepting(String state);

    /**
     * One-step transition from a single state under a symbol
     * (NFA style: returns a set).
     */
    Set<String> step(String state, String symbol);

    /**
     * One-step transition from a set of states under a symbol.
     * Useful when exploring product graph: (graphNode, currentStateSet).
     */
    Set<String> step(Set<String> states, String symbol);

    /**
     * Run the automaton on a full input sequence.
     * Returns the set of reachable states after consuming all symbols.
     */
    Set<String> run(Iterable<String> input);

    /**
     * Does the automaton accept this input?
     */
    boolean accepts(Iterable<String> input);
}
