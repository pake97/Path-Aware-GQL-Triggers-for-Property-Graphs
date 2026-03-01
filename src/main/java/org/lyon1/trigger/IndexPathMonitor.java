package org.lyon1.trigger;

import org.lyon1.automaton.Automaton;

import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IndexPathMonitor implements TriggerRegistryInterface.PathMonitor {

    private final Map<String, TriggerMachine> machines;
    // Fix 2: relType -> machines that care about it, built once at construction time
    private final Map<String, List<TriggerMachine>> machinesByRelType;
    private final org.neo4j.logging.Log log;

    private IndexPathMonitor(List<TriggerMachine> triggerMachines, org.neo4j.logging.Log log) {
        Map<String, TriggerMachine> machines = new java.util.HashMap<>();
        Map<String, List<TriggerMachine>> byRelType = new java.util.HashMap<>();
        for (TriggerMachine machine : triggerMachines) {
            machines.put(machine.getTriggerId(), machine);
            // Fix 2: index each machine under every relationship type it listens to
            for (String relType : machine.relevantRelTypes) {
                byRelType.computeIfAbsent(relType, k -> new java.util.ArrayList<>()).add(machine);
            }
        }
        this.machines = Map.copyOf(machines);
        this.machinesByRelType = java.util.Collections.unmodifiableMap(byRelType);
        this.log = log;
    }

    public static IndexPathMonitor fromMachines(List<TriggerMachine> triggerMachines, org.neo4j.logging.Log log) {
        return new IndexPathMonitor(triggerMachines, log);
    }

    @Override
    public Map<String, List<TriggerRegistryInterface.PathMatch>> findMatches(org.neo4j.graphdb.Transaction tx,
            String eventData) {
        Map<String, List<TriggerRegistryInterface.PathMatch>> matchesFound = new java.util.HashMap<>();

        if (eventData.startsWith("node:")) {
            String elementId = eventData.substring(5);
            try {
                org.neo4j.graphdb.Node node = tx.getNodeByElementId(elementId);
                for (TriggerMachine machine : machines.values()) {
                    if (node.hasLabel(org.neo4j.graphdb.Label.label(machine.initialLabel))) {
                        machine.getOrCreateTree(elementId, machine.initialState);
                    }
                }
            } catch (Exception e) {
            }
        } else if (eventData.startsWith("rel:")) {
            String elementId = eventData.substring(4);
            try {
                org.neo4j.graphdb.Relationship rel = tx.getRelationshipByElementId(elementId);
                String type = rel.getType().name();
                // Fix 2: only visit machines that declared this relationship type as relevant
                List<TriggerMachine> relevant = machinesByRelType.getOrDefault(type, List.of());
                if (!relevant.isEmpty()) {
                    org.neo4j.graphdb.Node start = rel.getStartNode();
                    org.neo4j.graphdb.Node end = rel.getEndNode();
                    for (TriggerMachine machine : relevant) {
                        // Check outgoing transitions: start -> end
                        processRelationship(tx, machine, rel, start, end, type, false, matchesFound);
                        // Check incoming transitions: end -> start
                        processRelationship(tx, machine, rel, end, start, type, true, matchesFound);
                    }
                }
            } catch (Exception e) {
            }
        }
        return matchesFound;
    }

    private void processRelationship(org.neo4j.graphdb.Transaction tx, TriggerMachine machine,
            org.neo4j.graphdb.Relationship rel,
            org.neo4j.graphdb.Node u, org.neo4j.graphdb.Node v,
            String type, boolean isIncoming, Map<String, List<TriggerRegistryInterface.PathMatch>> results) {
        String uId = u.getElementId();
        String vId = v.getElementId();
        String symbol = isIncoming ? "^" + type : type;

        // Find all states 's' that 'u' is currently in across all spanning trees
        for (String s : machine.getStatesForNode(uId)) {
            Set<String> nextStates = machine.automaton.step(s, symbol);
            for (String t : nextStates) {
                Set<SpanningTree> trees = machine.getTreesForNodeState(uId, s);
                for (SpanningTree tree : trees) {
                    expand(tx, machine, tree, new NodeState(uId, s), new NodeState(vId, t), rel, results);
                }
            }
        }

        // Also check if 'u' is the initial label root
        if (u.hasLabel(org.neo4j.graphdb.Label.label(machine.initialLabel))) {
            SpanningTree tree = machine.getOrCreateTree(uId, machine.initialState);
            Set<String> nextStates = machine.automaton.step(machine.initialState, symbol);
            for (String t : nextStates) {
                expand(tx, machine, tree, new NodeState(uId, machine.initialState), new NodeState(vId, t), rel,
                        results);
            }
        }
    }

    private void expand(org.neo4j.graphdb.Transaction tx, TriggerMachine machine, SpanningTree tree,
            NodeState u, NodeState v, org.neo4j.graphdb.Relationship e,
            Map<String, List<TriggerRegistryInterface.PathMatch>> results) {
        if (tree.contains(v))
            return;

        tree.addNode(v, u, e.getElementId());
        machine.addToInvertedIndex(v, tree);

        if (machine.automaton.isAccepting(v.state())) {
            TriggerRegistryInterface.PathMatch match = tree.reconstructPathMatch(v);
            results.computeIfAbsent(machine.triggerId, k -> new java.util.ArrayList<>()).add(match);

            if (log != null) {
                log.info("S-PATH Match! Triggered=" + machine.triggerId + " | Path=" + match);
            }
        }

        // Recursive Discovery
        try {
            org.neo4j.graphdb.Node vNode = tx.getNodeByElementId(v.elementId());
            for (org.neo4j.graphdb.Relationship nextRel : vNode.getRelationships()) {
                if (nextRel.equals(e))
                    continue;

                org.neo4j.graphdb.Node nextV = nextRel.getOtherNode(vNode);
                String type = nextRel.getType().name();

                // Outgoing from vNode
                String outSymbol = type;
                if (!nextRel.getStartNode().getElementId().equals(v.elementId())) {
                    outSymbol = "^" + type;
                }

                Set<String> nextStates = machine.automaton.step(v.state(), outSymbol);
                for (String q : nextStates) {
                    expand(tx, machine, tree, v, new NodeState(nextV.getElementId(), q), nextRel, results);
                }
            }
        } catch (Exception ex) {
        }
    }

    public record NodeState(String elementId, String state) {
    }

    public static class SpanningTree {
        final String rootId;
        final Map<NodeState, NodeState> parentPointers = new java.util.HashMap<>();
        final Map<NodeState, String> edgePointers = new java.util.HashMap<>();

        SpanningTree(String rootId, String initialState) {
            this.rootId = rootId;
            parentPointers.put(new NodeState(rootId, initialState), null);
        }

        boolean contains(NodeState ns) {
            return parentPointers.containsKey(ns);
        }

        void addNode(NodeState ns, NodeState parent, String edgeId) {
            parentPointers.put(ns, parent);
            edgePointers.put(ns, edgeId);
        }

        public TriggerRegistryInterface.PathMatch reconstructPathMatch(NodeState leaf) {
            java.util.List<String> nodeIds = new java.util.ArrayList<>();
            java.util.List<String> relIds = new java.util.ArrayList<>();
            NodeState curr = leaf;
            while (curr != null) {
                nodeIds.add(curr.elementId);
                String edgeId = edgePointers.get(curr);
                if (edgeId != null) {
                    relIds.add(edgeId);
                }
                curr = parentPointers.get(curr);
            }
            java.util.Collections.reverse(nodeIds);
            java.util.Collections.reverse(relIds);
            return new TriggerRegistryInterface.PathMatch(nodeIds, relIds);
        }

        String reconstructPath(NodeState leaf) {
            java.util.List<String> nodes = new java.util.ArrayList<>();
            NodeState curr = leaf;
            while (curr != null) {
                nodes.add(curr.elementId + ":" + curr.state);
                curr = parentPointers.get(curr);
            }
            java.util.Collections.reverse(nodes);
            return String.join(" -> ", nodes);
        }
    }

    public static class TriggerMachine {
        final String triggerId;
        final Automaton automaton;
        final String initialLabel;
        final String initialState;
        final Set<String> acceptingStates;
        // Fix 2: relationship types this machine cares about, used to build machinesByRelType
        final Set<String> relevantRelTypes;

        private final Map<String, SpanningTree> trees = new java.util.HashMap<>();
        // Fix 1: nodeId -> (nfaState -> Set<SpanningTree>) for O(1) lookup instead of O(n) scan
        private final Map<String, Map<String, Set<SpanningTree>>> invertedIndex = new java.util.HashMap<>();

        TriggerMachine(String triggerId, Automaton automaton, String initialLabel, String initialState,
                Set<String> acceptingStates, Set<String> relevantRelTypes) {
            this.triggerId = triggerId;
            this.automaton = automaton;
            this.initialLabel = initialLabel;
            this.initialState = initialState;
            this.acceptingStates = acceptingStates;
            this.relevantRelTypes = java.util.Collections.unmodifiableSet(relevantRelTypes);
        }

        public String getTriggerId() {
            return triggerId;
        }

        public SpanningTree getOrCreateTree(String rootId, String state) {
            return trees.computeIfAbsent(rootId, k -> {
                SpanningTree tree = new SpanningTree(rootId, state);
                addToInvertedIndex(new NodeState(rootId, state), tree);
                return tree;
            });
        }

        // Fix 1: nested put — O(1)
        void addToInvertedIndex(NodeState ns, SpanningTree tree) {
            invertedIndex
                    .computeIfAbsent(ns.elementId(), k -> new java.util.HashMap<>())
                    .computeIfAbsent(ns.state(), k -> new java.util.HashSet<>())
                    .add(tree);
        }

        // Fix 1: direct map lookup — O(1) instead of O(n) full scan
        Set<String> getStatesForNode(String elementId) {
            return invertedIndex.getOrDefault(elementId, java.util.Map.of()).keySet();
        }

        // Fix 1: two-level lookup — O(1)
        Set<SpanningTree> getTreesForNodeState(String elementId, String state) {
            return invertedIndex
                    .getOrDefault(elementId, java.util.Map.of())
                    .getOrDefault(state, java.util.Collections.emptySet());
        }
    }
}
