package org.lyon1.trigger;

import org.lyon1.automaton.Automaton;
import org.lyon1.automaton.GraphElementRef;
import org.lyon1.automaton.HashProductGraph;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IndexPathMonitor implements TriggerRegistryInterface.PathMonitor {

    private final Map<String,TriggerMachine> machines;

    private IndexPathMonitor(List<TriggerMachine> triggerMachines) {
        Map<String,TriggerMachine> machines = new java.util.HashMap<>();
        for(TriggerMachine machine : triggerMachines) {
            machines.put(machine.getTriggerId(), machine);
        }
        this.machines = Map.copyOf(machines);
    }

    public static IndexPathMonitor fromMachines(List<TriggerMachine> triggerMachines) {
        return new IndexPathMonitor(triggerMachines);
    }

    public Set<String> findMatchingTriggers(String indexChange) {
        // Implement your logic to find matching triggers based on index changes
        return Set.of();
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
