package org.lyon1.automaton;
import java.util.Set;
public interface AutomatonTransitionTableInterface {




        /**
         * Add a transition: from --symbol--> to
         */
        void addTransition(String from, String symbol, String to);

        /**
         * Get all target states for (from, symbol).
         * Supports NFA-style multiple targets.
         */
        Set<String> getNextStates(String from, String symbol);

        /**
         * All states that appear in this transition table.
         */
        Set<String> getStates();

        /**
         * All input symbols that appear in this transition table.
         */
        Set<String> getSymbols();

        /**
         * Remove a specific transition (if present).
         */
        void removeTransition(String from, String symbol, String to);

        /**
         * Remove all transitions from a given state under a symbol.
         */
        void clearTransitions(String from, String symbol);


}
