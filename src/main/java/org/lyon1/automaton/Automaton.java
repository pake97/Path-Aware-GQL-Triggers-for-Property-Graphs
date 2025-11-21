package org.lyon1.automaton;
import org.lyon1.path.GraphElement;
import org.lyon1.path.GraphPath;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Automaton implements AutomatonInterface {

    private AutomatonTransitionTable transitionTable;
    private final Set<String> initialStates = new HashSet<>();
    private final Set<String> acceptingStates = new HashSet<>();



    public Automaton(GraphPath path) {
        this.buildFromPath(path);
    }

    private void buildFromPath(GraphPath path) {
        this.addInitialState(path.get(0).getLabel());
        this.addAcceptingState(path.get(path.getNodes().size() + path.getRelationships().size() - 1).getLabel());
        for (int i = 0; i < path.getNodes().size() + path.getRelationships().size() - 1; i++) {
            String from = path.get(i).getLabel();
            String to = path.get(i + 1).getLabel();
            String symbol = path.get(i).getLabel();
            this.addTransition(from, symbol, to);
        }
    }

    // ---- Configuration API (builder-like, mutable) ----

    public Automaton addInitialState(String state) {
        initialStates.add(state);
        return this;
    }

    public Automaton setSingleInitialState(String state) {
        initialStates.clear();
        initialStates.add(state);
        return this;
    }

    public Automaton addAcceptingState(String state) {
        acceptingStates.add(state);
        return this;
    }

    public Automaton removeAcceptingState(String state) {
        acceptingStates.remove(state);
        return this;
    }

    // You can expose addTransition/… via delegating to transitionTable if useful:
    public Automaton addTransition(String from, String symbol, String to) {
        transitionTable.addTransition(from, symbol, to);
        return this;
    }

    // ---- Automaton interface implementation ----

    @Override
    public AutomatonTransitionTable getTransitionTable() {
        return transitionTable;
    }

    @Override
    public Set<String> getInitialStates() {
        return Collections.unmodifiableSet(initialStates);
    }

    @Override
    public Set<String> getAcceptingStates() {
        return Collections.unmodifiableSet(acceptingStates);
    }

    @Override
    public boolean isAccepting(String state) {
        return acceptingStates.contains(state);
    }

    @Override
    public Set<String> step(String state, String symbol) {
        return transitionTable.getNextStates(state, symbol);
    }

    @Override
    public Set<String> step(Set<String> states, String symbol) {
        if (states.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> next = new HashSet<>();
        for (String s : states) {
            next.addAll(step(s, symbol));
        }
        return Collections.unmodifiableSet(next);
    }

    @Override
    public Set<String> run(Iterable<String> input) {
        Set<String> current = new HashSet<>(initialStates);
        for (String symbol : input) {
            current = new HashSet<>(step(current, symbol));
            if (current.isEmpty()) {
                break;
            }
        }
        return Collections.unmodifiableSet(current);
    }

    @Override
    public boolean accepts(Iterable<String> input) {
        Set<String> finalStates = run(input);
        for (String s : finalStates) {
            if (isAccepting(s)) {
                return true;
            }
        }
        return false;
    }
}

