package org.lyon1.trigger.transition;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.*;

public final class TransitionTable {


    private final Map<String, OldNew<NodeSnapshot>> nodeTransitions;
    private final Map<String, OldNew<RelationshipSnapshot>> relTransitions;

    public TransitionTable(TransactionData data) {
        this.nodeTransitions = buildNodeTransitions(data);
        this.relTransitions  = buildRelTransitions(data);
    }

    public Map<String, OldNew<NodeSnapshot>> nodes() {
        return nodeTransitions;
    }

    public Map<String, OldNew<RelationshipSnapshot>> relationships() {
        return relTransitions;
    }

    public Optional<OldNew<NodeSnapshot>> node(String elementId) {
        return Optional.ofNullable(nodeTransitions.get(elementId));
    }

    public Optional<OldNew<RelationshipSnapshot>> relationship(String elementId) {
        return Optional.ofNullable(relTransitions.get(elementId));
    }

    // -------------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------------

    private Map<String, OldNew<NodeSnapshot>> buildNodeTransitions(TransactionData data) {
        Map<String, OldNew<NodeSnapshot>> result = new HashMap<>();

        // 1) "new" snapshots for created nodes
        for (Node n : data.createdNodes()) {
            String id = n.getElementId();
            NodeSnapshot nnew = new NodeSnapshot(id, labelsOf(n));   // your labelsOf(...) helper
            result.put(id, new OldNew<>(null, nnew));                 // old = null, new = created state
        }

        // 2) "old" snapshots for deleted nodes
        //    you already had a helper for reconstructing labels of deleted nodes
        Map<String, Set<String>> deletedNodeLabels = preDeleteLabels(data);

        for (Node n : data.deletedNodes()) {
            String id = n.getElementId();
            Set<String> oldLabels = deletedNodeLabels.getOrDefault(id, Set.of());
            NodeSnapshot old = new NodeSnapshot(id, oldLabels);
            result.put(id, new OldNew<>(old, null));                  // new = null, node is gone after tx
        }

        // 3) Optionally handle updated nodes (labels/properties changed but not created/deleted)
        //    Example with labels; you can extend this with property changes
        data.assignedLabels().forEach(al -> {
            Node n = al.node();
            String id = n.getElementId();
            // "old" labels == before assignment, "new" labels == after
            // to be precise you’d have to reconstruct both; for now we just treat it as "new"
            result.compute(id, (k, existing) -> {
                NodeSnapshot nnew = new NodeSnapshot(id, labelsOf(n));
                if (existing == null) {
                    return new OldNew<>(null, nnew);
                } else {
                    return new OldNew<>(existing.old(), nnew);
                }
            });
        });

        data.removedLabels().forEach(rl -> {
            Node n = rl.node();
            String id = n.getElementId();
            // Same idea: you could reconstruct old from deletedNodeLabels or dedicated logic
            result.computeIfAbsent(id, k -> new OldNew<>(null, null));
            OldNew<NodeSnapshot> existing = result.get(id);
            NodeSnapshot nnew = new NodeSnapshot(id, labelsOf(n));
            result.put(id, new OldNew<>(existing.old(), nnew));
        });

        return result;
    }

    private Map<String, OldNew<RelationshipSnapshot>> buildRelTransitions(TransactionData data) {
        Map<String, OldNew<RelationshipSnapshot>> result = new HashMap<>();

        // 1) created rels → old = null, new = snapshot after
        for (Relationship r : data.createdRelationships()) {
            String id = r.getElementId();
            RelationshipSnapshot nnew = new RelationshipSnapshot(id, r.getType().name());
            result.put(id, new OldNew<>(null, nnew));
        }

        // 2) deleted rels → old = snapshot before, new = null
        for (Relationship r : data.deletedRelationships()) {
            String id = r.getElementId();
            RelationshipSnapshot old = new RelationshipSnapshot(id, r.getType().name());
            result.put(id, new OldNew<>(old, null));
        }

        // If you care about relationship property changes, you’d enrich this here

        return result;
    }

    /* -------------------------------- Private utilities -------------------------------- */

    private static Set<String> labelsOf(Node n) {
        Set<String> out = new LinkedHashSet<>();
        for (Label l : n.getLabels()) out.add(l.name());
        return out;
    }

    /**
     * Build a map: nodeId -> labels that node had before it was deleted.
     * Neo4j reports label removals via removedLabels() also for node deletions.
     */
    private static Map<String, Set<String>> preDeleteLabels(TransactionData data) {
        Map<String, Set<String>> map = new HashMap<>(16);
        Set<String> deletedIds = new HashSet<>();
        for (Node n : data.deletedNodes()) deletedIds.add(n.getElementId());
        for (LabelEntry ev : data.removedLabels()) {
            String id = ev.node().getElementId();
            if (!deletedIds.contains(id)) continue; // only deletions
            map.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(ev.label().name());
        }
        return map;
    }
}


