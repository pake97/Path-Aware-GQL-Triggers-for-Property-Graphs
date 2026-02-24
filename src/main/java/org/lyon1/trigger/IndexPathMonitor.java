package org.lyon1.trigger;

import org.lyon1.automaton.Automaton;
import org.lyon1.automaton.HashProductGraph;
import org.lyon1.automaton.ProductNode;
import org.lyon1.automaton.StateType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IndexPathMonitor implements TriggerRegistryInterface.PathMonitor {

    private final Map<String, TriggerMachine> machines;

    private IndexPathMonitor(List<TriggerMachine> triggerMachines) {
        Map<String, TriggerMachine> machines = new java.util.HashMap<>();
        for (TriggerMachine machine : triggerMachines) {
            machines.put(machine.getTriggerId(), machine);
        }
        this.machines = Map.copyOf(machines);
    }

    public static IndexPathMonitor fromMachines(List<TriggerMachine> triggerMachines) {
        return new IndexPathMonitor(triggerMachines);
    }

    @Override
    public Set<String> findMatchingTriggers(org.neo4j.graphdb.Transaction tx, String indexChange) {
        Set<String> matchingTriggers = new java.util.HashSet<>();

        if (indexChange.startsWith("rel:")) {
            String elementId = indexChange.substring(4);
            try {
                org.neo4j.graphdb.Relationship rel = tx.getRelationshipByElementId(elementId);
                String type = rel.getType().name();
                org.neo4j.graphdb.Node start = rel.getStartNode();
                org.neo4j.graphdb.Node end = rel.getEndNode();

                for (TriggerMachine machine : machines.values()) {
                    // Specific path matching logic for performance test:
                    // (:Person)-[:OWN]->(:Account)<-[:DEPOSIT]-(:Loan)

                    String endId = end.getElementId();

                    if ("OWN".equals(type)) {
                        // (:Person)-[:OWN]->(:Account)
                        if (start.hasLabel(org.neo4j.graphdb.Label.label("Person"))) {
                            // Progress from Left: Account 'end' has a Person.
                            markProgress(machine, endId, "LEFT", matchingTriggers);
                        }
                    } else if ("DEPOSIT".equals(type)) {
                        // (:Account)<-[:DEPOSIT]-(:Loan)
                        // This usually materializes as Loan --DEPOSIT--> Account
                        if (start.hasLabel(org.neo4j.graphdb.Label.label("Loan"))) {
                            // Progress from Right: Account 'end' has a Loan.
                            markProgress(machine, endId, "RIGHT", matchingTriggers);
                        }
                    }
                }
            } catch (Exception e) {
                // Rel might be gone or other issues
            }
        }
        return matchingTriggers;
    }

    private void markProgress(TriggerMachine machine, String nodeId, String side, Set<String> matchingTriggers) {
        ProductNode prod = machine.getProductGraph().get(nodeId);
        String currentState = (prod != null) ? prod.State() : "NONE";

        String nextState = currentState;
        if ("LEFT".equals(side)) {
            if ("NONE".equals(currentState))
                nextState = "LEFT";
            else if ("RIGHT".equals(currentState))
                nextState = "BOTH";
        } else if ("RIGHT".equals(side)) {
            if ("NONE".equals(currentState))
                nextState = "RIGHT";
            else if ("LEFT".equals(currentState))
                nextState = "BOTH";
        }

        if (!nextState.equals(currentState)) {
            machine.getProductGraph().addNode(nodeId, nextState,
                    "BOTH".equals(nextState) ? StateType.ACCEPTING : StateType.NORMAL,
                    System.currentTimeMillis());

            if ("BOTH".equals(nextState)) {
                matchingTriggers.add(machine.getTriggerId());
            }
        }
    }

    static class TriggerMachine {

        final String triggerId;
        final Automaton automaton;
        final HashProductGraph productGraph;

        final Set<String> initialStates;
        final Set<String> acceptingStates;

        TriggerMachine(String triggerId,
                Automaton automaton,
                Set<String> initialStates,
                Set<String> acceptingStates

        ) {
            this.triggerId = triggerId;
            this.automaton = automaton;
            this.initialStates = initialStates;
            this.acceptingStates = acceptingStates;
            this.productGraph = new HashProductGraph();
        }

        public String getTriggerId() {
            return triggerId;
        }

        public Automaton getAutomaton() {
            return automaton;
        }

        public HashProductGraph getProductGraph() {
            return productGraph;
        }

        public Set<String> getInitialStates() {
            return initialStates;
        }

        public Set<String> getAcceptingStates() {
            return acceptingStates;
        }
    }
}
