package org.lyon1.trigger;

import org.lyon1.automaton.Automaton;
import org.lyon1.automaton.PatternParser;
import org.lyon1.path.GraphElement;
import org.lyon1.path.GraphPath;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.Log;

import java.util.*;

public final class IndexTriggerRegistry extends TriggerRegistry {

    public IndexTriggerRegistry(DatabaseManagementService dbms, Log log) {
        super(dbms, log);
    }

    @Override
    protected PathMonitor buildPathMonitor(Collection<Trigger> triggers) {

        List<IndexPathMonitor.TriggerMachine> machines = new java.util.ArrayList<>();

        for (Trigger trigger : triggers) {
            Activation activation = trigger.activation();
            if (trigger.scope() == Scope.PATH && trigger.enabled() && activation instanceof PathActivation pa) {

                String pathSignature = pa.canonicalSignature();
                GraphPath path = pathFromCanonicalSignature(pathSignature);
                Automaton automaton = new Automaton(path);

                String initialLabel = path.get(0).getLabel();
                String initialState = "S0";
                Set<String> acceptingStates = automaton.getAcceptingStates();

                // Fix 2: collect all relationship types referenced by this pattern
                Set<String> relevantRelTypes = new HashSet<>();
                for (GraphElement element : path.getElements()) {
                    if (element.isRelationship()) {
                        relevantRelTypes.add(element.getLabel());
                    }
                }

                IndexPathMonitor.TriggerMachine machine = new IndexPathMonitor.TriggerMachine(
                        trigger.id(), automaton, initialLabel, initialState, acceptingStates, relevantRelTypes);

                for (GraphElement element : path.getElements()) {
                    if (element.isRelationship())
                        continue;

                    String label = element.getLabel();
                    // Query to get all nodes with that label using elementId
                    Set<String> els = dbms.database("neo4j").executeTransactionally(
                            "MATCH (n:" + label + ") RETURN elementId(n) as id",
                            Collections.emptyMap(),
                            result -> {
                                Set<String> ids = new HashSet<>();
                                while (result.hasNext()) {
                                    Map<String, Object> row = result.next();
                                    ids.add(String.valueOf(row.get("id")));
                                }
                                return ids;
                            });

                    for (String nodeId : els) {
                        if (label.equals(initialLabel)) {
                            machine.getOrCreateTree(nodeId, initialState);
                        }
                    }
                }

                machines.add(machine);
            }
        }

        return IndexPathMonitor.fromMachines(machines, neoLog);
    }

    public Set<String> candidatesForPath(String label) {
        return Set.of();
    }

    GraphPath pathFromCanonicalSignature(String pathSignature) {
        return PatternParser.parsePattern(pathSignature);
    }
}
