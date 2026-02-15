package org.lyon1.trigger;

import java.util.*;

public final class AutomatonPathMonitor implements TriggerRegistryInterface.PathMonitor {

    private final Collection<TriggerRegistry.Trigger> triggers;
    private final org.neo4j.logging.Log log;

    private AutomatonPathMonitor(Collection<TriggerRegistry.Trigger> triggers, org.neo4j.logging.Log log) {
        this.triggers = triggers;
        this.log = log;
    }

    public static AutomatonPathMonitor fromTriggers(Collection<TriggerRegistry.Trigger> triggers,
            org.neo4j.logging.Log log) {
        return new AutomatonPathMonitor(triggers, log);
    }

    @Override
    public Set<String> findMatchingTriggers(org.neo4j.graphdb.Transaction tx, String elementIdWithPrefix) {
        if (elementIdWithPrefix == null || !elementIdWithPrefix.contains(":")) {
            return Set.of();
        }

        String[] parts = elementIdWithPrefix.split(":", 2);
        String type = parts[0]; // "node" or "rel"
        String elementId = parts[1];

        Set<String> matchingTriggerIds = new HashSet<>();

        for (TriggerRegistry.Trigger trigger : triggers) {
            if (trigger.scope() == TriggerRegistry.Scope.PATH && trigger.enabled()
                    && trigger.activation() instanceof TriggerRegistry.PathActivation pa) {
                if (matchesPath(tx, trigger, pa, type, elementId)) {
                    matchingTriggerIds.add(trigger.id());
                }
            }
        }

        return matchingTriggerIds;
    }

    private boolean matchesPath(org.neo4j.graphdb.Transaction tx, TriggerRegistry.Trigger trigger,
            TriggerRegistry.PathActivation pa, String elementType,
            String elementId) {
        String pattern = pa.canonicalSignature();
        // We need to check if the element (node or relationship) fits into the pattern.
        // The pattern is something like (:Person)-[:WORKS_AT]->(:Company)
        // We can use Cypher to check this by anchoring one element of the pattern to
        // the elementId.

        // For simplicity, we can try to "anchor" each node and relationship in the
        // pattern that matches the type.
        // A better approach would be to parse the pattern and only anchor relevant
        // positions.

        // Let's use a naive approach first:
        // 1. Convert the pattern to a Cypher MATCH clause.
        // 2. For nodes, try MATCH (n...)-... WHERE id(n) = $id, MATCH ...-(n...)-...
        // WHERE id(n) = $id, etc.
        // 3. For relationships, try MATCH ...-[r...]-... WHERE id(r) = $id.

        // Pattern Parser already exists, let's see if we can use it to get elements.
        // But for Cypher, we might just need the string pattern.

        // Example pattern: (n:Person)-[r:WORKS_AT]->(m:Company)
        // If it's a node:
        // MATCH (n:Person)-[:WORKS_AT]->(:Company) WHERE elementId(n) = $id RETURN
        // count(*) > 0
        // MATCH (:Person)-[:WORKS_AT]->(n:Company) WHERE elementId(n) = $id RETURN
        // count(*) > 0

        // If it's a relationship:
        // MATCH (:Person)-[r:WORKS_AT]->(:Company) WHERE elementId(r) = $id RETURN
        // count(*) > 0

        // We need to inject variables into the pattern to anchor them.
        // This is tricky if the pattern is already a complex string.

        // Let's assume the pattern looks like `(:A)-[:T]->(:B)`
        // We can replace the n-th node or relationship with `(n:A)` or `[r:T]` and add
        // `WHERE elementId(n/r) = $id`.

        // However, the simplest way is to use the existing pattern and just add a
        // MATCH.
        // But we need to identify WHICH node/relationship we are talking about.

        // Let's try to build queries by replacing each part of the pattern.
        // Actually, the user's `PatternParser` returns a `GraphPath` which contains
        // `GraphElement`s.

        org.lyon1.path.GraphPath path = org.lyon1.automaton.PatternParser.parsePattern(pattern);
        List<org.lyon1.path.GraphElement> elements = path.getElements();

        for (int i = 0; i < elements.size(); i++) {
            org.lyon1.path.GraphElement el = elements.get(i);
            boolean isNodeMatch = elementType.equals("node") && el.isNode();
            boolean isRelMatch = elementType.equals("rel") && el.isRelationship();

            if (isNodeMatch || isRelMatch) {
                // Try to anchor this specific element
                String query = buildAnchoredQuery(elements, i);
                if (executeCheck(tx, query, elementId)) {
                    return true;
                }
            }
        }

        return false;
    }

    private String buildAnchoredQuery(List<org.lyon1.path.GraphElement> elements, int anchorIndex) {
        StringBuilder sb = new StringBuilder("MATCH ");
        for (int i = 0; i < elements.size(); i++) {
            org.lyon1.path.GraphElement el = elements.get(i);
            String var = (i == anchorIndex) ? "anchor" : "";
            if (el.isNode()) {
                sb.append("(").append(var).append(":").append(el.getLabel()).append(")");
            } else {
                sb.append("-[").append(var).append(":").append(el.getLabel()).append("]->");
            }
        }
        sb.append(" WHERE elementId(anchor) = $id RETURN count(*) > 0 AS matched");
        return sb.toString();
    }

    private boolean executeCheck(org.neo4j.graphdb.Transaction tx, String query, String elementId) {
        try (org.neo4j.graphdb.Result result = tx.execute(query, Map.of("id", elementId))) {
            if (result.hasNext()) {
                return (Boolean) result.next().get("matched");
            }
            return false;
        } catch (Exception e) {
            if (log != null)
                log.error("Error executing path check Cypher query: " + query, e);
            return false;
        }
    }
}
