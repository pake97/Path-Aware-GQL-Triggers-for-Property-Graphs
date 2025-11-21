package org.lyon1.automaton;

import java.util.*;

public class AutomatonTransitionTable implements AutomatonTransitionTableInterface {

    // fromState -> (symbol -> {toStates})
    private final Map<String, Map<String, Set<String>>> table = new HashMap<>();

    private final Set<String> states = new HashSet<>();
    private final Set<String> symbols = new HashSet<>();

    @Override
    public void addTransition(String from, String symbol, String to) {
        states.add(from);
        states.add(to);
        symbols.add(symbol);

        table
                .computeIfAbsent(from, k -> new HashMap<>())
                .computeIfAbsent(symbol, k -> new HashSet<>())
                .add(to);
    }

    @Override
    public Set<String> getNextStates(String from, String symbol) {
        Map<String, Set<String>> bySymbol = table.get(from);
        if (bySymbol == null) {
            return Collections.emptySet();
        }
        Set<String> result = bySymbol.get(symbol);
        if (result == null) {
            return Collections.emptySet();
        }
        // Defensive copy to prevent external mutation
        return Collections.unmodifiableSet(result);
    }

    @Override
    public Set<String> getStates() {
        return Collections.unmodifiableSet(states);
    }


    @Override
    public Set<String> getSymbols() {
        return Collections.unmodifiableSet(symbols);
    }

    @Override
    public void removeTransition(String from, String symbol, String to) {
        Map<String, Set<String>> bySymbol = table.get(from);
        if (bySymbol == null) return;

        Set<String> targets = bySymbol.get(symbol);
        if (targets == null) return;

        targets.remove(to);
        if (targets.isEmpty()) {
            bySymbol.remove(symbol);
        }
        if (bySymbol.isEmpty()) {
            table.remove(from);
        }
        // Note: we keep states/symbols in their sets;
        // if you want "garbage collection" for unused states,
        // you'd need extra bookkeeping.
    }

    @Override
    public void clearTransitions(String from, String symbol) {
        Map<String, Set<String>> bySymbol = table.get(from);
        if (bySymbol == null) return;
        bySymbol.remove(symbol);
        if (bySymbol.isEmpty()) {
            table.remove(from);
        }
    }
}
