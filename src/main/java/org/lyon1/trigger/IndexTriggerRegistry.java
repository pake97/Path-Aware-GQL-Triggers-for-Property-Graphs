package org.lyon1.trigger;
import org.lyon1.automaton.Automaton;
import org.lyon1.automaton.GraphElementRef;
import org.lyon1.automaton.HashProductGraph;
import org.lyon1.automaton.PatternParser;
import org.lyon1.path.GraphElement;
import org.lyon1.path.GraphPath;
import org.neo4j.dbms.api.DatabaseManagementService;

import java.util.*;


public final class IndexTriggerRegistry extends TriggerRegistry {


    public IndexTriggerRegistry(DatabaseManagementService dbms){
        super(dbms);
    }

    @Override
    protected PathMonitor buildPathMonitor(Collection<Trigger> triggers) {

        Map<EventType, Map<String, Set<String>>> pathIndex  = new HashMap<>();

        Map<String,Set<String>> createEventMap = new HashMap<>();
        Map<String,Set<String>> deleteEventMap = new HashMap<>();

        pathIndex.put(EventType.ON_CREATE,deleteEventMap );
        pathIndex.put(EventType.ON_DELETE, createEventMap);

        List<IndexPathMonitor.TriggerMachine> machines = new java.util.ArrayList<>();



        for (Trigger trigger : triggers) {
            Activation activation = trigger.activation();
            if (trigger.scope() == Scope.PATH && trigger.enabled() && activation instanceof PathActivation pa) {

                String pathSignature = pa.canonicalSignature();
                GraphPath path = pathFromCanonicalSignature(pathSignature);
                Automaton automaton = new Automaton(path);

                Set<String> initialStates = automaton.getInitialStates();
                Set<String> acceptingStates = automaton.getAcceptingStates();

                HashProductGraph productGraph = new HashProductGraph();



                for(GraphElement element : path.getElements()) {
                    String label = element.getLabel();
                    //Query to get all nodes with that label
                    Set<String> els = dbms.database('neo4j').executeTransactionally(
                        "MATCH (n:" + label + ") RETURN id(n) as id",
                        Collections.emptyMap(),
                        result -> {
                            Set<String> ids = new HashSet<>();
                            while (result.hasNext()) {
                                Map<String, Object> row = result.next();
                                ids.add(String.valueOf(row.get("id")));
                            }
                            return ids;
                        }
                    );

                    productGraph.addNode(new GraphElement(els, label));

                }


                IndexPathMonitor.TriggerMachine machine =
                        new IndexPathMonitor.TriggerMachine(
                                trigger.id(), automaton, initialStates, acceptingStates
                        );

                machines.add(machine);
            }
        }

        // Whatever you need to build your automaton-based path monitor.
        // This is the ONLY part that differs for this subclass.
        return IndexPathMonitor.fromMachines(machines);
    }




    public Set<String> candidatesForPath(String label) {
        return Set.of();
    }





    GraphPath pathFromCanonicalSignature(String pathSignature) {

    return PatternParser.parsePattern(pathSignature);

    }
}
